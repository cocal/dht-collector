package com.cocal.dht;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Non-blocking stdout publisher for monitor.v1 records consumed by monitor-center-agent. */
final class MonitorLogger implements AutoCloseable {
  private static final int QUEUE_CAPACITY = 4_096;
  private static final DateTimeFormatter UTC = DateTimeFormatter.ISO_INSTANT;

  private final ObjectMapper mapper = new ObjectMapper();
  private final BlockingQueue<String> queue;
  private final PrintWriter output = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
  private final AtomicLong dropped = new AtomicLong();
  private final Thread writer;
  private volatile boolean closing;

  MonitorLogger() { this(QUEUE_CAPACITY); }

  MonitorLogger(int queueCapacity) {
    if (queueCapacity < 1) throw new IllegalArgumentException("monitor queue capacity must be positive");
    queue = new ArrayBlockingQueue<>(queueCapacity);
    writer = Thread.ofVirtual().name("dht-monitor-log-writer").start(this::drain);
  }

  void metric(String metric, long value) { metric(metric, value, null); }

  void metric(String metric, long value, String query) {
    if (closing || value <= 0) return;
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("schema", "monitor.v1");
    event.put("event_id", UUID.randomUUID().toString());
    event.put("service", "dht-collector");
    event.put("metric", metric);
    event.put("occurred_at", UTC.format(Instant.now()));
    event.put("value", value);
    if (query != null && !query.isBlank()) event.put("query", query);
    try {
      if (!queue.offer(mapper.writeValueAsString(event))) dropped.incrementAndGet();
    } catch (JsonProcessingException error) {
      System.err.println("monitor log serialization failed: " + error.getMessage());
    }
  }

  long dropped() { return dropped.get(); }

  private void drain() {
    try {
      while (!closing || !queue.isEmpty()) {
        String line = queue.poll(200, TimeUnit.MILLISECONDS);
        if (line == null) continue;
        synchronized (output) {
          output.println(line);
          output.flush();
        }
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void close() {
    if (closing) return;
    closing = true;
    try { writer.join(3_000); }
    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    long lost = dropped.get();
    if (lost > 0) System.err.println("monitor log queue dropped " + lost + " record(s)");
  }
}
