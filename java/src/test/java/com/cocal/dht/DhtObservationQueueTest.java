package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

final class DhtObservationQueueTest {
  @Test
  void coalescesAHashAndUpgradesItToAnnouncePriority() throws Exception {
    DhtObservationQueue queue = new DhtObservationQueue(4);
    Instant first = Instant.parse("2026-08-14T10:00:00Z");
    InetSocketAddress peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 51413);

    assertTrue(queue.offer(observation("a", first, "get_peers", null)));
    assertTrue(queue.offer(observation("b", first, "get_peers", null)));
    assertTrue(queue.offer(observation("b", first.plusSeconds(1), "announce_peer", peer)));

    List<DhtObservation> batch = queue.drain(4, 1);
    assertEquals(List.of("b", "a"), batch.stream().map(DhtObservation::infoHash).toList());
    assertTrue(batch.getFirst().isAnnounce());
    assertEquals(peer, batch.getFirst().peer());
  }

  @Test
  void fullQueueKeepsFreshAnnounceAheadOfGetPeers() throws Exception {
    DhtObservationQueue queue = new DhtObservationQueue(2);
    Instant now = Instant.parse("2026-08-14T10:00:00Z");
    InetSocketAddress peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 51413);
    queue.offer(observation("old", now, "get_peers", null));
    queue.offer(observation("newer", now.plusSeconds(1), "get_peers", null));

    assertTrue(queue.offer(observation("live", now.plusSeconds(2), "announce_peer", peer)));
    List<String> hashes = queue.drain(4, 1).stream().map(DhtObservation::infoHash).toList();

    assertEquals(List.of("live", "newer"), hashes);
    assertEquals(1, queue.droppedSinceLastReport());
    assertTrue(queue.isEmpty());
  }

  private static DhtObservation observation(String hash, Instant at, String query,
                                            InetSocketAddress peer) {
    return new DhtObservation(hash, at, query, peer,
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 49000));
  }
}
