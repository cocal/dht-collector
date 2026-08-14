package com.cocal.dht;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/** A validated monitor.v1 point accepted by the independent monitor ingest path. */
record MonitorPoint(String eventId, String metric, String query, Instant occurredAt, long value) {
  static Optional<MonitorPoint> parse(Map<String, Object> event) {
    if (!"monitor.v1".equals(text(event.get("schema")))) return Optional.empty();
    String eventId = text(event.get("event_id")).trim();
    String metric = text(event.get("metric")).trim();
    String occurred = text(event.get("occurred_at")).trim();
    if (eventId.isBlank() || metric.isBlank() || !metric.matches("[a-z0-9][a-z0-9_.-]{0,95}")
        || occurred.isBlank()) return Optional.empty();
    Instant timestamp;
    try { timestamp = Instant.parse(occurred); }
    catch (RuntimeException ignored) { return Optional.empty(); }
    long value;
    try {
      Object rawValue = event.get("value");
      value = rawValue instanceof Number number ? number.longValue() : Long.parseLong(text(rawValue));
    } catch (RuntimeException ignored) { return Optional.empty(); }
    if (value <= 0) return Optional.empty();
    String query = text(event.get("query")).trim();
    return Optional.of(new MonitorPoint(eventId, metric, query, timestamp, value));
  }

  /** Return links, queries, failures, warnings, and newly indexed contents for the minute aggregate. */
  long[] delta() {
    return switch (metric) {
      case "dht.resource_discovered" -> new long[]{value, 0, 0, 0, 0};
      case "dht.query", "dht.query_summary" -> new long[]{0, value, 0, 0, 0};
      case "metadata.fetch_failed", "collector.failed", "dht.error" -> new long[]{0, 0, value, 0, 0};
      case "dht.warning" -> new long[]{0, 0, 0, value, 0};
      case "content.indexed" -> new long[]{0, 0, 0, 0, value};
      default -> new long[]{0, 0, 0, 0, 0};
    };
  }

  private static String text(Object value) { return value == null ? "" : String.valueOf(value); }
}
