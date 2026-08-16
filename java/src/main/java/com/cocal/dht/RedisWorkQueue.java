package com.cocal.dht;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

/** Redis Streams transport. PostgreSQL remains the durable source of business state. */
final class RedisWorkQueue implements AutoCloseable {
  static final String OBSERVATIONS = "dht:observations";
  static final String METADATA_TASKS = "dht:metadata-tasks";
  static final String METADATA_DEAD = "dht:metadata-dead";
  static final String OBSERVATION_GROUP = "dht-db-writers";
  static final String TASK_GROUP = "dht-metadata-workers";
  private static final long OBSERVATION_MAXLEN = 250_000;
  private static final long TASK_MAXLEN = 100_000;
  private static final long TASK_DEDUPE_SECONDS = 900;
  private static final long CLAIM_IDLE_MILLIS = 120_000;

  record Message(StreamEntryID id, Map<String, String> fields) {}
  record Task(StreamEntryID id, String infoHash, int priority) {}

  private final JedisPooled redis;

  RedisWorkQueue(JedisPooled redis) {
    if (redis == null) throw new IllegalArgumentException("Redis client is required");
    this.redis = redis;
  }

  void ensureObservationGroup() {
    ensureGroup(OBSERVATIONS, OBSERVATION_GROUP);
  }

  void ensureTaskGroup() {
    ensureGroup(METADATA_TASKS, TASK_GROUP);
  }

  void publishObservation(DhtObservation observation) {
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("kind", "observation");
    fields.put("event_id", observation.eventId());
    fields.put("info_hash", observation.infoHash());
    fields.put("observed_at", observation.observedAt().toString());
    fields.put("query", observation.query());
    if (observation.peer() != null) {
      fields.put("peer_host", observation.peer().getHostString());
      fields.put("peer_port", Integer.toString(observation.peer().getPort()));
    }
    if (observation.source() != null) {
      fields.put("source_host", observation.source().getHostString());
      fields.put("source_port", Integer.toString(observation.source().getPort()));
    }
    redis.xadd(OBSERVATIONS, XAddParams.xAddParams().maxLen(OBSERVATION_MAXLEN).approximateTrimming(), fields);
  }

  List<Message> readObservations(String consumer, int count) {
    ensureObservationGroup();
    List<Message> result = new ArrayList<>();
    try {
      var claimed = redis.xautoclaim(OBSERVATIONS, OBSERVATION_GROUP, consumer,
          CLAIM_IDLE_MILLIS, StreamEntryID.MINIMUM_ID, XAutoClaimParams.xAutoClaimParams().count(count));
      for (StreamEntry entry : claimed.getValue()) result.add(new Message(entry.getID(), entry.getFields()));
    } catch (RuntimeException ignored) { }
    if (!result.isEmpty()) return result;
    var batches = redis.xreadGroup(OBSERVATION_GROUP, consumer,
        XReadGroupParams.xReadGroupParams().count(count).block(1000),
        Map.of(OBSERVATIONS, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));
    if (batches != null) {
      for (var batch : batches) for (StreamEntry entry : batch.getValue()) {
        result.add(new Message(entry.getID(), entry.getFields()));
      }
    }
    return result;
  }

  void ackObservation(StreamEntryID id) {
    redis.xack(OBSERVATIONS, OBSERVATION_GROUP, id);
    redis.xdel(OBSERVATIONS, id);
  }

  boolean publishTask(String infoHash, int priority) {
    if (infoHash == null || infoHash.isBlank()) return false;
    String dedupeKey = "dht:task-enqueued:" + infoHash;
    String token = UUID.randomUUID().toString();
    if (!"OK".equals(redis.setex(dedupeKey, TASK_DEDUPE_SECONDS, token))) return false;
    try {
      Map<String, String> fields = Map.of("kind", "metadata", "info_hash", infoHash,
          "priority", Integer.toString(priority), "queued_at", Instant.now().toString());
      redis.xadd(METADATA_TASKS, XAddParams.xAddParams().maxLen(TASK_MAXLEN).approximateTrimming(), fields);
      return true;
    } catch (RuntimeException error) {
      redis.del(dedupeKey);
      throw error;
    }
  }

  void publishDeadTask(String infoHash, String message) {
    redis.xadd(METADATA_DEAD, XAddParams.xAddParams().maxLen(10_000).approximateTrimming(),
        Map.of("kind", "metadata-dead", "info_hash", infoHash, "failed_at", Instant.now().toString(),
            "error", message == null ? "unknown" : message.substring(0, Math.min(2_000, message.length()))));
  }

  List<Task> readTasks(String consumer, int count) {
    ensureTaskGroup();
    List<Task> result = new ArrayList<>();
    try {
      var claimed = redis.xautoclaim(METADATA_TASKS, TASK_GROUP, consumer,
          CLAIM_IDLE_MILLIS, StreamEntryID.MINIMUM_ID, XAutoClaimParams.xAutoClaimParams().count(count));
      for (StreamEntry entry : claimed.getValue()) addTask(result, entry);
    } catch (RuntimeException ignored) { }
    if (!result.isEmpty()) return result;
    var batches = redis.xreadGroup(TASK_GROUP, consumer,
        XReadGroupParams.xReadGroupParams().count(count).block(1000),
        Map.of(METADATA_TASKS, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));
    if (batches != null) for (var batch : batches) for (StreamEntry entry : batch.getValue()) addTask(result, entry);
    return result;
  }

  void ackTask(Task task) {
    redis.xack(METADATA_TASKS, TASK_GROUP, task.id());
    redis.xdel(METADATA_TASKS, task.id());
    redis.del("dht:task-enqueued:" + task.infoHash());
  }

  long observationDepth() { return redis.xlen(OBSERVATIONS); }

  private static void addTask(List<Task> target, StreamEntry entry) {
    Map<String, String> fields = entry.getFields();
    if (!"metadata".equals(fields.get("kind"))) return;
    try {
      target.add(new Task(entry.getID(), fields.get("info_hash"), Integer.parseInt(fields.getOrDefault("priority", "0"))));
    } catch (RuntimeException ignored) { }
  }

  private void ensureGroup(String stream, String group) {
    try {
      redis.xgroupCreate(stream, group, StreamEntryID.XGROUP_LAST_ENTRY, true);
    } catch (RuntimeException error) {
      if (!String.valueOf(error.getMessage()).contains("BUSYGROUP")) {
        redis.xadd(stream, XAddParams.xAddParams().maxLen(10).approximateTrimming(), Map.of("kind", "init"));
        try { redis.xgroupCreate(stream, group, StreamEntryID.XGROUP_LAST_ENTRY, true); }
        catch (RuntimeException retry) {
          if (!String.valueOf(retry.getMessage()).contains("BUSYGROUP")) throw retry;
        }
      }
    }
  }

  static DhtObservation decodeObservation(Map<String, String> fields) {
    if (!"observation".equals(fields.get("kind"))) return null;
    try {
      return new DhtObservation(fields.get("event_id"), fields.get("info_hash"),
          Instant.parse(fields.get("observed_at")), fields.get("query"),
          endpoint(fields, "peer"), endpoint(fields, "source"));
    } catch (RuntimeException error) {
      return null;
    }
  }

  private static InetSocketAddress endpoint(Map<String, String> fields, String prefix) {
    String host = fields.get(prefix + "_host");
    String port = fields.get(prefix + "_port");
    if (host == null || port == null) return null;
    int value = Integer.parseInt(port);
    if (value < 1 || value > 65535) return null;
    return new InetSocketAddress(host, value);
  }

  @Override public void close() { }
}
