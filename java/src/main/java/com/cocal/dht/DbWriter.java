package com.cocal.dht;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import redis.clients.jedis.JedisPooled;

/** Single logical PostgreSQL writer for distributed DHT observation intake. */
final class DbWriter implements AutoCloseable {
  private static final int BATCH_SIZE = 128;
  private final Catalog catalog;
  private final JedisPooled redis;
  private final RedisWorkQueue queue;
  private final MonitorLogger monitor = new MonitorLogger();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private final String consumer = System.getenv().getOrDefault("DHT_NODE_ID", "writer-" + ProcessHandle.current().pid());

  DbWriter(Catalog catalog, JedisPooled redis) {
    this.catalog = catalog;
    this.redis = redis;
    this.queue = new RedisWorkQueue(redis);
  }

  void run() {
    queue.ensureObservationGroup();
    queue.ensureTaskGroup();
    long nextDispatch = 0;
    while (!stopping.get()) {
      try {
        List<RedisWorkQueue.Message> messages = queue.readObservations(consumer, BATCH_SIZE);
        if (!messages.isEmpty()) {
          persist(messages);
          nextDispatch = 0;
        }
        if (System.nanoTime() >= nextDispatch) {
          dispatchDueJobs();
          nextDispatch = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        }
        if (messages.isEmpty()) Thread.sleep(100);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception error) {
        monitor.metric("db_writer.failed", 1);
        System.err.println("db writer batch failed; pending messages will be retried: " + error.getMessage());
        try { Thread.sleep(1_000); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
      }
    }
  }

  private void persist(List<RedisWorkQueue.Message> messages) throws Exception {
    List<DhtObservation> observations = new ArrayList<>();
    for (RedisWorkQueue.Message message : messages) {
      DhtObservation observation = RedisWorkQueue.decodeObservation(message.fields());
      if (observation != null) observations.add(observation);
      else queue.ackObservation(message.id());
    }
    if (observations.isEmpty()) return;
    Catalog.ObservationWrite write = catalog.persistObservations(observations);
    for (String hash : write.freshHashes()) queue.publishTask(hash, 10);
    for (String hash : write.immediateHashes()) queue.publishTask(hash, DhtCollector.LIVE_METADATA_PRIORITY);
    for (RedisWorkQueue.Message message : messages) queue.ackObservation(message.id());
    monitor.metric("db_writer.observations", observations.size());
    if (!write.freshHashes().isEmpty()) monitor.metric("db_writer.tasks_enqueued", write.freshHashes().size());
  }

  private void dispatchDueJobs() throws Exception {
    for (Map.Entry<String, Integer> job : catalog.dueMetadataJobs(256, Instant.now())) {
      queue.publishTask(job.getKey(), job.getValue());
    }
  }

  @Override public void close() {
    if (!stopping.compareAndSet(false, true)) return;
    queue.close();
    redis.close();
    monitor.close();
  }
}
