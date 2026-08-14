package com.cocal.dht;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Short-lived negative cache scoped to one info-hash and peer endpoint. */
final class PeerPenaltyCache {
  private static final int MAX_ENTRIES = 65_536;

  private record Target(String infoHash, InetSocketAddress peer) {}

  private final Map<Target, Long> penalizedUntil = new ConcurrentHashMap<>();

  boolean isPenalized(String infoHash, InetSocketAddress peer, long nowMillis) {
    Target target = new Target(infoHash, peer);
    Long until = penalizedUntil.get(target);
    if (until == null) return false;
    if (until > nowMillis) return true;
    penalizedUntil.remove(target, until);
    return false;
  }

  void penalize(String infoHash, InetSocketAddress peer, long nowMillis, long ttlMillis) {
    if (ttlMillis <= 0) return;
    if (penalizedUntil.size() >= MAX_ENTRIES) prune(nowMillis);
    if (penalizedUntil.size() >= MAX_ENTRIES) {
      var iterator = penalizedUntil.keySet().iterator();
      if (iterator.hasNext()) penalizedUntil.remove(iterator.next());
    }
    penalizedUntil.merge(new Target(infoHash, peer), nowMillis + ttlMillis, Math::max);
  }

  void clear(String infoHash, InetSocketAddress peer) {
    penalizedUntil.remove(new Target(infoHash, peer));
  }

  int size() { return penalizedUntil.size(); }

  void prune(long nowMillis) {
    penalizedUntil.entrySet().removeIf(entry -> entry.getValue() <= nowMillis);
  }
}
