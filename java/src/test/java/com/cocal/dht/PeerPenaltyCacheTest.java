package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;

final class PeerPenaltyCacheTest {
  @Test
  void penaltyIsScopedToInfoHashAndExpires() {
    PeerPenaltyCache cache = new PeerPenaltyCache();
    InetSocketAddress peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 51413);
    cache.penalize("hash-a", peer, 1_000, 5_000);

    assertTrue(cache.isPenalized("hash-a", peer, 5_999));
    assertFalse(cache.isPenalized("hash-b", peer, 5_999));
    assertFalse(cache.isPenalized("hash-a", peer, 6_000));
  }

  @Test
  void clearAllowsImmediateRetry() {
    PeerPenaltyCache cache = new PeerPenaltyCache();
    InetSocketAddress peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 51413);
    cache.penalize("hash-a", peer, 1_000, 5_000);
    cache.clear("hash-a", peer);

    assertFalse(cache.isPenalized("hash-a", peer, 1_001));
  }
}
