package com.cocal.dht;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Small in-process ranking of endpoints that have recently served metadata. */
final class PeerScoreCache {
  private static final int MAX_ENTRIES = 65_536;
  private final Map<InetSocketAddress, Integer> scores = new ConcurrentHashMap<>();

  int score(InetSocketAddress peer) {
    return scores.getOrDefault(peer, 0);
  }

  void success(InetSocketAddress peer) {
    scores.compute(peer, (ignored, value) -> Math.min(100, (value == null ? 0 : value) + 4));
    trim();
  }

  void failure(InetSocketAddress peer) {
    scores.compute(peer, (ignored, value) -> Math.max(-20, (value == null ? 0 : value) - 1));
    trim();
  }

  private void trim() {
    if (scores.size() <= MAX_ENTRIES) return;
    scores.entrySet().stream().sorted(Map.Entry.comparingByValue()).limit(scores.size() - MAX_ENTRIES)
        .map(Map.Entry::getKey).forEach(scores::remove);
  }
}
