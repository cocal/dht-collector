package com.cocal.dht;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;

final class DhtCollector implements AutoCloseable {
  static final long CACHE_TTL_MS = Duration.ofHours(24).toMillis();
  static final long ANNOUNCE_PEER_TTL_MS = Duration.ofMinutes(10).toMillis();

  private record PeerHint(InetSocketAddress address, long observedAt) {}

  private final Config config;
  private final Catalog catalog;
  private final RecentResourceCache cache;
  private final ArrayList<DHT> nodes = new ArrayList<>();
  private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Semaphore permits;
  private final Semaphore metadataPermits;
  private final MetadataFetcher metadata;
  private final Map<String, Instant> pendingTouches = new ConcurrentHashMap<>();
  private final Map<String, Map<InetSocketAddress, PeerHint>> announcedPeers = new ConcurrentHashMap<>();
  private final Map<String, Boolean> runningMetadata = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> queryCounts = new ConcurrentHashMap<>();
  private final AtomicLong discovered;
  private volatile boolean stopping;

  DhtCollector(Config config, Catalog catalog) throws Exception {
    this.config = config;
    this.catalog = catalog;
    this.cache = new RecentResourceCache(CACHE_TTL_MS);
    catalog.loadRecentResources(Instant.now().minus(Duration.ofHours(24)), cache::load);
    this.discovered = new AtomicLong(
        config.maxResources() > 0 ? catalog.countDiscoveredResources() : 0);
    this.permits = new Semaphore(config.maxConcurrent());
    this.metadataPermits = new Semaphore(config.metadataConcurrent());
    Files.createDirectories(config.storagePath());
    try {
      for (int index = 0; index < config.dhtNodes(); index++) {
        int port = config.port() + index;
        DHT dht = new DHT(DHTtype.IPV4_DHT);
        dht.addIncomingMessageListener(this::onIncomingMessage);
        dht.start(configuration(port, config.storagePath().resolve("node-" + port)));
        nodes.add(dht);
      }
      metadata = new MetadataFetcher(nodes, config.metadataConcurrent(), config.metadataTimeoutSeconds());
    } catch (Exception error) {
      nodes.forEach(DHT::stop);
      scheduler.shutdownNow();
      tasks.close();
      throw error;
    }
  }

  void start() {
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        flushTouches();
        cache.prune(System.currentTimeMillis());
        flushQueries();
      } catch (Exception error) {
        System.err.println("scheduled catalog flush failed: " + error.getMessage());
      }
    }, 30, 30, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(() -> {
      try { pollMetadataJobs(); }
      catch (Exception error) { System.err.println("metadata job poll failed: " + error.getMessage()); }
    }, 5, 5, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(() -> {
      try { catalog.markInvalidResources(Instant.now().minus(Duration.ofDays(7))); }
      catch (Exception error) { System.err.println("resource expiry sweep failed: " + error.getMessage()); }
    }, 1, 1, TimeUnit.HOURS);
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
  }

  int cachedResources() { return cache.size(); }

  private DHTConfiguration configuration(int port, Path storagePath) throws Exception {
    Files.createDirectories(storagePath);
    InetAddress configuredAddress = InetAddress.getByName(config.address());
    Predicate<InetAddress> addressFilter = candidate -> configuredAddress.isAnyLocalAddress()
        ? candidate.isAnyLocalAddress() || candidate.getAddress().length == 4
        : configuredAddress.equals(candidate);
    return new DHTConfiguration() {
      @Override public boolean isPersistingID() { return true; }
      @Override public Path getStoragePath() { return storagePath; }
      @Override public int getListeningPort() { return port; }
      @Override public boolean noRouterBootstrap() { return false; }
      @Override public boolean allowMultiHoming() { return false; }
      @Override public Predicate<InetAddress> filterBindAddress() { return addressFilter; }
    };
  }

  private void onIncomingMessage(DHT dht, MessageBase message) {
    String query;
    String infoHash;
    if (message instanceof GetPeersRequest request) {
      query = "get_peers";
      infoHash = request.getInfoHash().toString(false).toLowerCase(Locale.ROOT);
    } else if (message instanceof AnnounceRequest request) {
      query = "announce_peer";
      infoHash = request.getInfoHash().toString(false).toLowerCase(Locale.ROOT);
      InetSocketAddress origin = request.getOrigin();
      int peerPort = request.getPort();
      if (origin != null && peerPort > 0 && peerPort <= 65535) {
        InetSocketAddress peer = new InetSocketAddress(origin.getAddress(), peerPort);
        announcedPeers.compute(infoHash, (ignored, existing) -> {
          Map<InetSocketAddress, PeerHint> peers = existing == null ? new LinkedHashMap<>() : new LinkedHashMap<>(existing);
          peers.put(peer, new PeerHint(peer, System.currentTimeMillis()));
          while (peers.size() > 3) peers.remove(peers.keySet().iterator().next());
          return peers;
        });
      }
    } else {
      return;
    }
    queryCounts.computeIfAbsent(query, ignored -> new LongAdder()).increment();
    if (!permits.tryAcquire()) return;
    tasks.submit(() -> {
      try {
        acceptResource(infoHash, query);
        if (query.equals("announce_peer")) startMetadataForAnnounce(infoHash);
      } catch (Exception error) {
        System.err.println("resource observation failed: " + error.getMessage());
      } finally {
        permits.release();
      }
    });
  }

  private void acceptResource(String infoHash, String query) throws Exception {
    long nowMillis = System.currentTimeMillis();
    Instant now = Instant.ofEpochMilli(nowMillis);
    if (cache.contains(infoHash, nowMillis)) {
      cache.observe(infoHash, nowMillis);
      pendingTouches.put(infoHash, now);
      if (query.equals("announce_peer")) catalog.queueMetadataJob(infoHash, now, 100, true);
      return;
    }
    if (config.maxResources() > 0 && discovered.get() >= config.maxResources() && !catalog.exists(infoHash)) return;
    cache.observe(infoHash, nowMillis);
    boolean fresh;
    try {
      fresh = catalog.claim(infoHash, now, query);
    } catch (Exception error) {
      cache.remove(infoHash);
      throw error;
    }
    if (fresh) {
      discovered.incrementAndGet();
      catalog.event("dht.resource_discovered", infoHash,
          "{\"event\":\"dht.resource_discovered\",\"info_hash\":\"" + infoHash
              + "\",\"query\":\"" + query + "\"}");
    }
    catalog.queueMetadataJob(infoHash, now, query.equals("announce_peer") ? 100 : 10, query.equals("announce_peer"));
  }

  private void flushTouches() throws Exception {
    if (pendingTouches.isEmpty()) return;
    Map<String, Instant> copy = new HashMap<>(pendingTouches);
    catalog.touch(copy);
    copy.forEach((hash, observedAt) -> pendingTouches.remove(hash, observedAt));
  }

  private void pollMetadataJobs() throws Exception {
    int capacity = metadataPermits.availablePermits();
    if (capacity == 0) return;
    for (String infoHash : catalog.claimMetadataJobs(capacity, Instant.now())) {
      if (!runningMetadata.putIfAbsent(infoHash, Boolean.TRUE)) continue;
      if (!metadataPermits.tryAcquire()) return;
      tasks.submit(() -> {
        try {
          Collection<InetSocketAddress> preferredPeers = currentAnnouncePeers(infoHash);
          metadata.fetch(infoHash, preferredPeers).toCompletableFuture()
              .get(config.metadataTimeoutSeconds() + 5L, TimeUnit.SECONDS)
              .ifPresentOrElse(manifest -> completeMetadataSuccess(infoHash, manifest),
                  () -> completeMetadataFailure(infoHash, "no metadata result"));
        } catch (Exception error) {
          completeMetadataFailure(infoHash, error.getMessage());
        } finally {
          runningMetadata.remove(infoHash);
          metadataPermits.release();
        }
      });
    }
  }

  private void startMetadataForAnnounce(String infoHash) {
    if (!runningMetadata.putIfAbsent(infoHash, Boolean.TRUE)) return;
    if (!metadataPermits.tryAcquire()) {
      runningMetadata.remove(infoHash);
      return;
    }
    tasks.submit(() -> {
      try {
        metadata.fetch(infoHash, currentAnnouncePeers(infoHash)).toCompletableFuture()
            .get(config.metadataTimeoutSeconds() + 5L, TimeUnit.SECONDS)
            .ifPresentOrElse(manifest -> completeMetadataSuccess(infoHash, manifest),
                () -> completeMetadataFailure(infoHash, "no metadata result"));
      } catch (Exception error) {
        completeMetadataFailure(infoHash, error.getMessage());
      } finally {
        runningMetadata.remove(infoHash);
        metadataPermits.release();
      }
    });
  }

  private Collection<InetSocketAddress> currentAnnouncePeers(String infoHash) {
    long cutoff = System.currentTimeMillis() - ANNOUNCE_PEER_TTL_MS;
    Map<InetSocketAddress, PeerHint> hints = announcedPeers.get(infoHash);
    if (hints == null) return List.of();
    return hints.values().stream().filter(hint -> hint.observedAt() >= cutoff).map(PeerHint::address).toList();
  }

  private void completeMetadataSuccess(String infoHash, Manifest manifest) {
    try {
      catalog.upsertManifest(manifest);
      catalog.event("metadata.fetch_completed", infoHash,
          "{\"event\":\"metadata.fetch_completed\",\"info_hash\":\"" + infoHash
              + "\",\"name\":\"" + jsonEscape(manifest.name()) + "\"}", "passive");
      catalog.completeMetadataJob(infoHash, true, Instant.now());
    } catch (Exception error) {
      System.err.println("metadata persistence failed: " + error.getMessage());
    }
  }

  private void completeMetadataFailure(String infoHash, String message) {
    try {
      catalog.event("metadata.fetch_failed", infoHash,
          "{\"event\":\"metadata.fetch_failed\",\"info_hash\":\"" + infoHash
              + "\",\"message\":\"" + jsonEscape(message == null ? "unknown" : message) + "\"}", "passive");
      catalog.completeMetadataJob(infoHash, false, Instant.now());
    } catch (Exception error) {
      System.err.println("metadata failure persistence failed: " + error.getMessage());
    }
  }

  private static String jsonEscape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r");
  }

  private void flushQueries() throws Exception {
    for (var entry : queryCounts.entrySet()) {
      long count = entry.getValue().sumThenReset();
      if (count > 0) {
        catalog.event("dht.query_summary", null,
            "{\"event\":\"dht.query_summary\",\"query\":\"" + entry.getKey()
                + "\",\"occurrences\":" + count + ",\"interval_seconds\":30}");
      }
    }
  }

  @Override public void close() {
    if (stopping) return;
    stopping = true;
    nodes.forEach(DHT::stop);
    metadata.close();
    scheduler.shutdownNow();
    tasks.close();
    try {
      flushTouches();
      flushQueries();
    } catch (Exception error) {
      System.err.println("final catalog flush failed: " + error.getMessage());
    }
  }
}
