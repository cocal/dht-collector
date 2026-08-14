package com.cocal.dht;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

record DhtObservation(String infoHash, Instant observedAt, String query,
                      InetSocketAddress peer, InetSocketAddress source) {
  boolean isAnnounce() { return "announce_peer".equals(query); }

  DhtObservation merge(DhtObservation newer) {
    if (!infoHash.equals(newer.infoHash)) throw new IllegalArgumentException("cannot merge different info-hashes");
    if (newer.isAnnounce()) return newer;
    if (isAnnounce()) {
      return new DhtObservation(infoHash, newer.observedAt.isAfter(observedAt) ? newer.observedAt : observedAt,
          query, peer, source);
    }
    return newer.observedAt.isAfter(observedAt) ? newer : this;
  }
}

/** Bounded, coalescing queue that gives fresh announce_peer observations priority. */
final class DhtObservationQueue {
  private final int capacity;
  private final ArrayDeque<String> order = new ArrayDeque<>();
  private final Map<String, DhtObservation> pending = new HashMap<>();
  private long dropped;
  private boolean closing;

  DhtObservationQueue(int capacity) {
    if (capacity < 1) throw new IllegalArgumentException("observation queue capacity must be positive");
    this.capacity = capacity;
  }

  synchronized boolean offer(DhtObservation observation) {
    if (closing) return false;
    DhtObservation existing = pending.get(observation.infoHash());
    if (existing != null) {
      DhtObservation merged = existing.merge(observation);
      pending.put(observation.infoHash(), merged);
      if (!existing.isAnnounce() && merged.isAnnounce()) {
        order.remove(observation.infoHash());
        order.addFirst(observation.infoHash());
      }
      return true;
    }
    if (pending.size() >= capacity) {
      if (!observation.isAnnounce()) {
        dropped++;
        return false;
      }
      String evicted = findOldestGetPeers();
      if (evicted == null) evicted = order.pollLast();
      else order.remove(evicted);
      if (evicted != null) pending.remove(evicted);
      dropped++;
    }
    pending.put(observation.infoHash(), observation);
    if (observation.isAnnounce()) order.addFirst(observation.infoHash());
    else order.addLast(observation.infoHash());
    notifyAll();
    return true;
  }

  synchronized List<DhtObservation> drain(int limit, long waitMillis) throws InterruptedException {
    if (limit < 1) throw new IllegalArgumentException("drain limit must be positive");
    if (pending.isEmpty() && !closing) wait(Math.max(1L, waitMillis));
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(waitMillis);
    while (!closing && !pending.isEmpty() && pending.size() < limit) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) break;
      long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
      int nanos = (int) (remaining - java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(millis));
      wait(millis, nanos);
    }
    List<DhtObservation> result = new ArrayList<>(Math.min(limit, pending.size()));
    while (result.size() < limit) {
      String hash = order.pollFirst();
      if (hash == null) break;
      DhtObservation observation = pending.remove(hash);
      if (observation != null) result.add(observation);
    }
    return List.copyOf(result);
  }

  synchronized void requeue(List<DhtObservation> observations) {
    for (int index = observations.size() - 1; index >= 0; index--) offer(observations.get(index));
  }

  synchronized int size() { return pending.size(); }

  synchronized long droppedSinceLastReport() {
    long result = dropped;
    dropped = 0;
    return result;
  }

  synchronized boolean isEmpty() { return pending.isEmpty(); }

  synchronized void close() {
    closing = true;
    notifyAll();
  }

  private String findOldestGetPeers() {
    var iterator = order.iterator();
    while (iterator.hasNext()) {
      String hash = iterator.next();
      DhtObservation candidate = pending.get(hash);
      if (candidate != null && !candidate.isAnnounce()) return hash;
    }
    return null;
  }
}
