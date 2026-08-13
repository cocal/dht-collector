package com.cocal.dht;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

  private final Config config;
  private final Catalog catalog;
  private final RecentResourceCache cache;
  private final ArrayList<DHT> nodes = new ArrayList<>();
  private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Semaphore permits;
  private final Map<String, Instant> pendingTouches = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> queryCounts = new ConcurrentHashMap<>();
  private final AtomicLong discovered;
  private volatile boolean stopping;

  DhtCollector(Config config, Catalog catalog) throws Exception {
    this.config = config;
    this.catalog = catalog;
    var recent = catalog.recentResources(Instant.now().minus(Duration.ofHours(24)));
    this.cache = new RecentResourceCache(CACHE_TTL_MS, recent);
    this.discovered = new AtomicLong(
        config.maxResources() > 0 ? catalog.countDiscoveredResources() : 0);
    this.permits = new Semaphore(config.maxConcurrent());
    Files.createDirectories(config.storagePath());
    try {
      for (int index = 0; index < config.dhtNodes(); index++) {
        int port = config.port() + index;
        DHT dht = new DHT(DHTtype.IPV4_DHT);
        dht.addIncomingMessageListener(this::onIncomingMessage);
        dht.start(configuration(port, config.storagePath().resolve("node-" + port)));
        nodes.add(dht);
      }
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
    } else {
      return;
    }
    queryCounts.computeIfAbsent(query, ignored -> new LongAdder()).increment();
    if (!permits.tryAcquire()) return;
    tasks.submit(() -> {
      try {
        acceptResource(infoHash, query);
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
  }

  private void flushTouches() throws Exception {
    if (pendingTouches.isEmpty()) return;
    Map<String, Instant> copy = new HashMap<>(pendingTouches);
    catalog.touch(copy);
    copy.forEach((hash, observedAt) -> pendingTouches.remove(hash, observedAt));
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
