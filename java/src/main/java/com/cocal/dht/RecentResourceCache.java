package com.cocal.dht;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class RecentResourceCache {
  private final long ttlMillis;
  private final Map<String, Long> observed = new ConcurrentHashMap<>();
  RecentResourceCache(long ttlMillis) { this.ttlMillis = ttlMillis; }
  RecentResourceCache(long ttlMillis, Map<String, Instant> initial) {
    this(ttlMillis);
    initial.forEach(this::load);
    prune(System.currentTimeMillis());
  }
  void load(String hash, Instant time) { observed.put(hash, time.toEpochMilli()); }
  boolean contains(String hash, long now) {
    Long seen = observed.get(hash);
    if (seen == null) return false;
    if (seen < now - ttlMillis) { observed.remove(hash, seen); return false; }
    return true;
  }
  void observe(String hash, long now) { observed.put(hash, now); }
  void remove(String hash) { observed.remove(hash); }
  int prune(long now) { int removed = 0; long cutoff = now - ttlMillis; for (var entry : observed.entrySet()) if (entry.getValue() < cutoff && observed.remove(entry.getKey(), entry.getValue())) removed++; return removed; }
  int size() { return observed.size(); }
}
