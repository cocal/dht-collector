package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RecentResourceCacheTest {
  @Test void expiresEntriesAfterTwentyFourHours() {
    long now = System.currentTimeMillis();
    RecentResourceCache cache = new RecentResourceCache(24 * 60 * 60 * 1000L, Map.of("a", Instant.ofEpochMilli(now - 1), "b", Instant.ofEpochMilli(now - 24 * 60 * 60 * 1000L - 1)));
    assertEquals(1, cache.size());
    assertTrue(cache.contains("a", now));
    assertFalse(cache.contains("b", now));
  }
}
