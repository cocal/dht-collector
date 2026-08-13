package com.cocal.dht;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;

final class DhtRuntime implements AutoCloseable {
  interface PeerObserver { void accept(InetSocketAddress peer, InetSocketAddress source) throws Exception; }
  record LookupResult(int peers, int requests, int responses, long elapsedMillis) {}

  private final DHT dht = new DHT(DHTtype.IPV4_DHT);
  private final Path storage;
  private final String address;
  private final int port;

  DhtRuntime(String address, int port, Path storage) {
    this.address = address;
    this.port = port;
    this.storage = storage;
  }

  void start() throws Exception {
    Files.createDirectories(storage);
    dht.start(configuration());
    dht.getServerManager().awaitActiveServer().get(15, TimeUnit.SECONDS);
  }

  DHT dht() { return dht; }

  Map<String,Object> status() {
    Map<String,Object> status = new LinkedHashMap<>();
    status.put("routing_nodes", dht.getNode().getNumEntriesInRoutingTable());
    status.put("received_packets", dht.getStats().getNumReceivedPackets());
    status.put("sent_packets", dht.getStats().getNumSentPackets());
    status.put("listen_port", port);
    return status;
  }

  LookupResult lookup(String infoHash, Duration timeout, PeerObserver observer) throws Exception {
    PeerLookupTask task = dht.createPeerLookup(new Key(infoHash).getHash());
    task.setNoAnnounce(true);
    task.setFastTerminate(false);
    java.util.Set<InetSocketAddress> peers = java.util.concurrent.ConcurrentHashMap.newKeySet();
    java.util.concurrent.atomic.AtomicReference<Exception> callbackError = new java.util.concurrent.atomic.AtomicReference<>();
    task.setResultHandler((source, item) -> {
      InetSocketAddress peer = item.toSocketAddress();
      if (!peers.add(peer)) return;
      try { observer.accept(peer, source == null ? null : source.getAddress()); }
      catch (Exception error) { callbackError.compareAndSet(null, error); }
    });
    CompletableFuture<Void> done = new CompletableFuture<>();
    task.addListener(ignored -> done.complete(null));
    long started = System.nanoTime();
    task.start();
    try { done.get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
    catch (java.util.concurrent.TimeoutException error) { task.kill(); throw new IllegalStateException("lookup timed out after " + timeout.toMillis() + "ms", error); }
    if (callbackError.get() != null) throw callbackError.get();
    return new LookupResult(peers.size(), task.getSentReqs(), task.getRecvResponses(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
  }

  private DHTConfiguration configuration() throws Exception {
    InetAddress configuredAddress = InetAddress.getByName(address);
    Predicate<InetAddress> addressFilter = candidate -> configuredAddress.isAnyLocalAddress()
        ? candidate.isAnyLocalAddress() || candidate.getAddress().length == 4
        : configuredAddress.equals(candidate);
    return new DHTConfiguration() {
      @Override public boolean isPersistingID() { return true; }
      @Override public Path getStoragePath() { return storage; }
      @Override public int getListeningPort() { return port; }
      @Override public boolean noRouterBootstrap() { return false; }
      @Override public boolean allowMultiHoming() { return false; }
      @Override public Predicate<InetAddress> filterBindAddress() { return addressFilter; }
    };
  }

  @Override public void close() { dht.stop(); }
}
