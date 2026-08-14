package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MonitorPointTest {
  @Test void acceptsUtcMonitorRecordAndMapsQuery() {
    var point = MonitorPoint.parse(Map.of(
        "schema", "monitor.v1",
        "event_id", "event-1",
        "metric", "dht.query_summary",
        "occurred_at", "2026-08-13T15:44:17.213288Z",
        "value", 12,
        "query", "get_peers")).orElseThrow();
    assertEquals(Instant.parse("2026-08-13T15:44:17.213288Z"), point.occurredAt());
    assertEquals("get_peers", point.query());
    assertArrayEquals(new long[]{0, 12, 0, 0, 0}, point.delta());
  }

  @Test void rejectsLocalTimeAndUnknownMetricDoesNotEnterTrend() {
    assertTrue(MonitorPoint.parse(Map.of(
        "schema", "monitor.v1", "event_id", "event-2", "metric", "dht.query",
        "occurred_at", "2026-08-13 23:44:17", "value", 1)).isEmpty());
    var point = MonitorPoint.parse(Map.of(
        "schema", "monitor.v1", "event_id", "event-3", "metric", "metadata.fetch_completed",
        "occurred_at", "2026-08-13T15:44:17Z", "value", 1)).orElseThrow();
    assertArrayEquals(new long[]{0, 0, 0, 0, 0}, point.delta());
    var indexed = MonitorPoint.parse(Map.of(
        "schema", "monitor.v1", "event_id", "event-4", "metric", "content.indexed",
        "occurred_at", "2026-08-13T15:44:17Z", "value", 3)).orElseThrow();
    assertArrayEquals(new long[]{0, 0, 0, 0, 3}, indexed.delta());
  }
}
