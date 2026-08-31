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
import java.util.LinkedHashSet;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;
import redis.clients.jedis.JedisPooled;

final class DhtCollector implements AutoCloseable {
  static final long CACHE_TTL_MS = Duration.ofHours(2).toMillis();
  static final long ANNOUNCE_PEER_TTL_MS = Duration.ofMinutes(10).toMillis();
  static final int LIVE_METADATA_PRIORITY = 100;
  static final int LIVE_METADATA_TIMEOUT_SECONDS = 30;
  static final int LIVE_DIRECT_TIMEOUT_SECONDS = 10;
  static final int DIRECT_FALLBACK_PRIORITY = LIVE_METADATA_PRIORITY;
  static final int DIRECT_FALLBACK_DELAY_SECONDS = 5;
  static final int RECENT_LOOKUP_PRIORITY = 50;
  // Retain more recent announce endpoints so transiently bad peers do not
  // exhaust the metadata attempt before a usable endpoint is tried.
  static final int MAX_ANNOUNCE_ENDPOINTS = 12;
  static final int MAX_ANNOUNCED_PEER_HASHES = 50_000;
  static final int MAX_PENDING_TOUCHES = 50_000;
  // Keep intake transactions short. A larger batch makes probe_event index/page
  // reads hold the connection for seconds when the table is under write load.
  static final int OBSERVATION_BATCH_SIZE = 64;
  static final long OBSERVATION_FLUSH_MILLIS = 100;
  private static final long INCOMING_NODE_PROBE_INTERVAL_NANOS =
      TimeUnit.MILLISECONDS.toNanos(250);

  private record PeerHint(InetSocketAddress address, long observedAt, long sequence) {}
  private record PeerSnapshot(List<InetSocketAddress> addresses, long sequence) {}

  private final Config config;
  private final Catalog catalog;
  private final MonitorLogger monitor;
  private final RecentResourceCache cache;
  private final ArrayList<DHT> nodes = new ArrayList<>();
  private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Semaphore metadataPermits;
  // DHT fallback builds a lookup graph, unlike direct announce peers. Keep it
  // independently bounded so it cannot starve the direct metadata worker pool.
  private final Semaphore hotFallbackPermits;
  private final DhtObservationQueue observationQueue;
  private final MetadataFetcher metadata;
  private final JedisPooled redis;
  private final RedisWorkQueue workQueue;
  private final ObservationSpool observationSpool;
  private final String taskConsumer;
  private final Map<String, Instant> pendingTouches = new ConcurrentHashMap<>();
  private final Map<String, Map<InetSocketAddress, PeerHint>> announcedPeers = new ConcurrentHashMap<>();
  private final Map<String, Boolean> runningMetadata = new ConcurrentHashMap<>();
  private final Map<String, Boolean> runningDirectMetadata = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> queryCounts = new ConcurrentHashMap<>();
  private final Map<DHT, AtomicLong> incomingNodeProbeGates = new ConcurrentHashMap<>();
  private final AtomicLong peerSequence = new AtomicLong();
  private final AtomicBoolean metadataRecoveryRunning = new AtomicBoolean();
  private final AtomicLong metadataRecoveryAttemptNanos = new AtomicLong();
  private final AtomicLong discovered;
  private volatile Thread observationWriter;
  private volatile Thread metadataQueueReader;
  private volatile boolean stopping;

  DhtCollector(Config config, Catalog catalog) throws Exception {
    this.config = config;
    this.catalog = catalog;
    this.cache = new RecentResourceCache(CACHE_TTL_MS);
    this.monitor = new MonitorLogger();
    this.redis = connectRedis(config.redisUrl());
    this.workQueue = redis == null ? null : new RedisWorkQueue(redis);
    this.taskConsumer = System.getenv().getOrDefault("DHT_NODE_ID",
        "collector-" + config.port() + "-" + ProcessHandle.current().pid());
    try {
      this.discovered = new AtomicLong(
          config.maxResources() > 0 ? catalog.countDiscoveredResources() : 0);
    } catch (Exception error) {
      monitor.close();
      if (redis != null) redis.close();
      throw error;
    }
    this.observationQueue = new DhtObservationQueue(Math.max(2_048, config.maxConcurrent() * 64));
    this.metadataPermits = new Semaphore(config.metadataConcurrent());
    this.hotFallbackPermits = new Semaphore(1);
    Files.createDirectories(config.storagePath());
    try {
      this.observationSpool = redis == null ? null : new ObservationSpool(config.storagePath().resolve("observation-spool.db"));
    } catch (Exception error) {
      if (redis != null) redis.close();
      throw error;
    }
    try {
      for (int index = 0; index < config.dhtNodes(); index++) {
        int port = config.port() + index;
        DHT dht = new DHT(DHTtype.IPV4_DHT);
        dht.addIncomingMessageListener(this::onIncomingMessage);
        dht.start(configuration(port, config.storagePath().resolve("node-" + port)));
        seedConfiguredBootstrap(dht);
        nodes.add(dht);
      }
      metadata = new MetadataFetcher(nodes, config.metadataConcurrent(), config.metadataTimeoutSeconds(),
          monitor::metric);
    } catch (Exception error) {
      nodes.forEach(DHT::stop);
      scheduler.shutdownNow();
      tasks.close();
      monitor.close();
      if (redis != null) redis.close();
      throw error;
    }
  }

  void start() {
    if (observationWriter == null) {
      observationWriter = Thread.ofVirtual().name("dht-observation-writer").start(this::drainObservations);
    }
    if (workQueue != null && metadataQueueReader == null) {
      workQueue.ensureTaskGroup();
      metadataQueueReader = Thread.ofVirtual().name("dht-metadata-queue-reader").start(this::consumeMetadataTasks);
    }
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        flushTouches();
        cache.prune(System.currentTimeMillis());
        pruneAnnouncedPeers(System.currentTimeMillis());
        flushQueries();
      } catch (Exception error) {
        monitor.metric("collector.failed", 1);
        System.err.println("scheduled catalog flush failed: " + error.getMessage());
      }
    }, 30, 30, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(() -> {
      try { catalog.flushCounters(); }
      catch (Exception error) {
        monitor.metric("collector.failed", 1);
        System.err.println("catalog counter flush failed: " + error.getMessage());
      }
    }, 5, 5, TimeUnit.SECONDS);
    if (workQueue == null) {
      scheduler.scheduleWithFixedDelay(() -> {
        try { pollMetadataJobs(); }
        catch (Exception error) {
          monitor.metric("collector.failed", 1);
          System.err.println("metadata job poll failed: " + error.getMessage());
        }
      }, 5, 5, TimeUnit.SECONDS);
    }
    scheduler.scheduleWithFixedDelay(() -> {
      long routingNodes = nodes.stream().mapToLong(node -> node.getNode().getNumEntriesInRoutingTable()).sum();
      metadata.reapIfOverloaded();
      if (routingNodes > 0) monitor.metric("dht.routing_nodes", routingNodes);
      long activeTasks = metadata.activeDhtTasks();
      long queuedTasks = metadata.queuedDhtTasks();
      if (activeTasks > 0) monitor.metric("metadata.dht_tasks_active", activeTasks);
      if (queuedTasks > 0) monitor.metric("metadata.dht_tasks_queued", queuedTasks);
      int observationDepth = observationQueue.size();
      if (observationDepth > 0) monitor.metric("collector.observation_queue_depth", observationDepth);
      long droppedObservations = observationQueue.droppedSinceLastReport();
      if (droppedObservations > 0) monitor.metric("collector.observation_dropped", droppedObservations);
    }, 15, 15, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(() -> {
      try { catalog.markInvalidResources(Instant.now().minus(Duration.ofDays(7))); }
      catch (Exception error) {
        monitor.metric("collector.failed", 1);
        System.err.println("resource expiry sweep failed: " + error.getMessage());
      }
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
    InetSocketAddress peerEndpoint = null;
    InetSocketAddress sourceEndpoint = message.getOrigin();
    if (message instanceof GetPeersRequest request) {
      query = "get_peers";
      infoHash = request.getInfoHash().toString(false).toLowerCase(Locale.ROOT);
    } else if (message instanceof AnnounceRequest request) {
      query = "announce_peer";
      infoHash = request.getInfoHash().toString(false).toLowerCase(Locale.ROOT);
      InetSocketAddress origin = request.getOrigin();
      int peerPort = request.getPort();
      List<InetSocketAddress> endpoints = announceEndpoints(origin, peerPort);
      if (!endpoints.isEmpty()) {
        peerEndpoint = endpoints.getFirst();
        rememberAnnouncedPeers(infoHash, endpoints);
      }
    } else {
      return;
    }
    InetSocketAddress persistedPeer = peerEndpoint;
    InetSocketAddress persistedSource = sourceEndpoint;
    seedRoutingTable(dht, sourceEndpoint);
    queryCounts.computeIfAbsent(query, ignored -> new LongAdder()).increment();
    enqueueObservation(new DhtObservation(infoHash, Instant.now(), query, persistedPeer, persistedSource));
  }

  private void enqueueObservation(DhtObservation observation) {
    long observedAt = observation.observedAt().toEpochMilli();
    if (workQueue != null) {
      observationQueue.offer(observation);
      return;
    }
    if (cache.contains(observation.infoHash(), observedAt)) {
      cache.observe(observation.infoHash(), observedAt);
      if (!observation.isAnnounce()) {
        queuePendingTouch(observation.infoHash(), observation.observedAt());
        return;
      }
    } else if (config.maxResources() > 0 && discovered.get() >= config.maxResources()) {
      return;
    }
    observationQueue.offer(observation);
  }

  private void drainObservations() {
    while (!stopping || !observationQueue.isEmpty()) {
      List<DhtObservation> batch;
      try {
        batch = observationQueue.drain(OBSERVATION_BATCH_SIZE, OBSERVATION_FLUSH_MILLIS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return;
      }
      if (batch.isEmpty()) continue;
      try {
        if (workQueue != null) {
          publishQueuedObservations(batch);
          long queuedAt = System.currentTimeMillis();
          for (DhtObservation observation : batch) {
            cache.observe(observation.infoHash(), Math.max(queuedAt, observation.observedAt().toEpochMilli()));
          }
          monitor.metric("collector.observation_enqueued", batch.size());
          continue;
        }
        Catalog.ObservationWrite write = catalog.persistObservations(batch);
        long persistedAt = System.currentTimeMillis();
        for (DhtObservation observation : batch) {
          cache.observe(observation.infoHash(), Math.max(persistedAt, observation.observedAt().toEpochMilli()));
        }
        int fresh = write.freshHashes().size();
        if (fresh > 0) {
          discovered.addAndGet(fresh);
          monitor.metric("dht.resource_discovered", fresh);
        }
        if (write.peerFacts() > 0) {
          monitor.metric("dht.peer_discovered", write.peerFacts());
        }
        if (!stopping) write.immediateHashes().forEach(this::startMetadataForAnnounce);
      } catch (Exception error) {
        monitor.metric("collector.failed", 1);
        monitor.metric("collector.observation_retry", batch.size());
        System.err.println("observation batch persistence failed: " + error.getMessage());
        if (stopping) return;
        observationQueue.requeue(batch);
        try { Thread.sleep(500); }
        catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void publishQueuedObservations(List<DhtObservation> batch) throws Exception {
    if (observationSpool != null && workQueue.observationDepth() < 50_000) {
      List<DhtObservation> replay = observationSpool.read(Math.min(OBSERVATION_BATCH_SIZE, batch.size()));
      if (!replay.isEmpty()) {
        try {
          for (DhtObservation observation : replay) workQueue.publishObservation(observation);
          observationSpool.remove(replay);
        } catch (RuntimeException error) {
          observationSpool.append(batch);
          return;
        }
      }
    }
    if (workQueue.observationDepth() >= 100_000) {
      observationSpool.append(batch);
      monitor.metric("collector.observation_spooled", batch.size());
      return;
    }
    try {
      for (DhtObservation observation : batch) workQueue.publishObservation(observation);
    } catch (RuntimeException error) {
      observationSpool.append(batch);
      monitor.metric("collector.observation_spooled", batch.size());
    }
  }

  private void flushTouches() throws Exception {
    if (workQueue != null) return;
    if (pendingTouches.isEmpty()) return;
    Map<String, Instant> copy = new HashMap<>(pendingTouches);
    catalog.touch(copy);
    copy.forEach((hash, observedAt) -> pendingTouches.remove(hash, observedAt));
  }

  private void queuePendingTouch(String infoHash, Instant observedAt) {
    if (pendingTouches.containsKey(infoHash)) {
      pendingTouches.merge(infoHash, observedAt,
          (existing, newer) -> newer.isAfter(existing) ? newer : existing);
      return;
    }
    if (pendingTouches.size() >= MAX_PENDING_TOUCHES) {
      monitor.metric("collector.pending_touches_dropped", 1);
      return;
    }
    pendingTouches.putIfAbsent(infoHash, observedAt);
  }

  private void consumeMetadataTasks() {
    while (!stopping) {
      int capacity = metadataPermits.availablePermits();
      if (capacity < 1) {
        try { Thread.sleep(250); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
        continue;
      }
      try {
        List<RedisWorkQueue.Task> tasks = workQueue.readTasks(taskConsumer, Math.min(16, capacity));
        if (tasks.isEmpty()) continue;
        for (RedisWorkQueue.Task task : tasks) {
          if (!metadataPermits.tryAcquire()) break;
          launchQueuedMetadataTask(task);
        }
      } catch (Exception error) {
        monitor.metric("collector.failed", 1);
        System.err.println("metadata stream poll failed: " + error.getMessage());
        try { Thread.sleep(1_000); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
      }
    }
  }

  private void launchQueuedMetadataTask(RedisWorkQueue.Task task) {
    String infoHash = task.infoHash();
    TaskLease lease = TaskLease.tryAcquire(redis, infoHash);
    if (lease == null || runningMetadata.putIfAbsent(infoHash, Boolean.TRUE) != null) {
      if (lease != null) lease.close();
      metadataPermits.release();
      return;
    }
    tasks.submit(() -> {
      boolean acknowledge = false;
      try {
        if (!catalog.claimImmediateMetadataJob(infoHash, Instant.now())) {
          acknowledge = true;
          return;
        }
        Map<String, List<InetSocketAddress>> persisted = catalog.recentPeerHints(
            List.of(infoHash), Instant.now().minusMillis(ANNOUNCE_PEER_TTL_MS), 12);
        Collection<InetSocketAddress> peers = mergePeerHints(currentAnnouncePeers(infoHash), persisted.get(infoHash));
        int timeoutSeconds = task.priority() >= LIVE_METADATA_PRIORITY
            ? liveDirectTimeoutSeconds() : liveMetadataTimeoutSeconds();
        var manifest = task.priority() >= LIVE_METADATA_PRIORITY
            ? fetchHotMetadata(infoHash, peers, timeoutSeconds)
            : metadata.fetch(infoHash, peers, timeoutSeconds).toCompletableFuture()
                .get(timeoutSeconds + 5L, TimeUnit.SECONDS);
        acknowledge = manifest.isPresent()
            ? completeMetadataSuccess(infoHash, manifest.get())
            : completeMetadataFailure(infoHash, "no metadata result");
      } catch (Exception error) {
        acknowledge = completeMetadataFailure(infoHash, error.getMessage());
      } finally {
        if (acknowledge) workQueue.ackTask(task);
        lease.close();
        runningMetadata.remove(infoHash);
        metadataPermits.release();
      }
    });
  }

  private void pollMetadataJobs() throws Exception {
    long nowNanos = System.nanoTime();
    long previousAttempt = metadataRecoveryAttemptNanos.get();
    if (nowNanos - previousAttempt >= TimeUnit.SECONDS.toNanos(30)
        && metadataRecoveryAttemptNanos.compareAndSet(previousAttempt, nowNanos)
        && metadataRecoveryRunning.compareAndSet(false, true)) {
      tasks.submit(() -> {
        try {
          int recovered = catalog.recoverExpiredMetadataJobs(Instant.now());
          if (recovered > 0) monitor.metric("metadata.jobs_recovered", recovered);
        } catch (Exception error) {
          System.err.println("metadata lease recovery deferred: " + error.getMessage());
        } finally {
          metadataRecoveryRunning.set(false);
        }
      });
    }

    // Announce-triggered work acquires the same semaphore asynchronously. Use every
    // currently free slot for high-priority jobs first, then leave one slot available
    // for a new announce while draining the large fallback queue.
    int liveCapacity = metadataPermits.availablePermits();
    if (liveCapacity > 0) launchMetadataJobs(liveCapacity, LIVE_METADATA_PRIORITY, liveMetadataTimeoutSeconds());
    // Probe only a tiny number of newly observed get_peers resources after direct
    // announce work. Candidate selection remains bounded to recent indexed rows.
    int lookupCapacity = 0;
    if (lookupCapacity > 0) launchMetadataJobs(lookupCapacity, RECENT_LOOKUP_PRIORITY,
        LIVE_METADATA_PRIORITY, liveMetadataTimeoutSeconds());
  }

  private void launchMetadataJobs(int capacity, int minimumPriority, int timeoutSeconds) throws Exception {
    launchMetadataJobs(capacity, minimumPriority, Integer.MAX_VALUE, timeoutSeconds);
  }

  private void launchMetadataJobs(int capacity, int minimumPriority, int maximumPriority, int timeoutSeconds) throws Exception {
    int reserved = 0;
    while (reserved < capacity && metadataPermits.tryAcquire()) reserved++;
    if (reserved == 0) return;
    int remaining = reserved;
    try {
      Instant claimedAt = Instant.now();
      List<String> claimed = catalog.claimMetadataJobs(reserved, claimedAt, minimumPriority, maximumPriority);
      Map<String, List<InetSocketAddress>> persistedPeers;
      try {
        persistedPeers = catalog.recentPeerHints(claimed,
            claimedAt.minusMillis(ANNOUNCE_PEER_TTL_MS), 12);
      } catch (Exception error) {
        persistedPeers = Map.of();
        monitor.metric("collector.failed", 1);
        System.err.println("recent peer recovery failed: " + error.getMessage());
      }
      final Map<String, List<InetSocketAddress>> recoveredPeers = persistedPeers;
      for (String infoHash : claimed) {
        remaining--;
        TaskLease lease = TaskLease.tryAcquire(redis, infoHash);
        if (redis != null && lease == null) {
          catalog.queueMetadataJob(infoHash, Instant.now().plusSeconds(5), minimumPriority, true);
          metadataPermits.release();
          continue;
        }
        if (runningMetadata.putIfAbsent(infoHash, Boolean.TRUE) != null) {
          if (lease != null) lease.close();
          catalog.queueMetadataJob(infoHash, Instant.now().plusSeconds(5), minimumPriority, true);
          metadataPermits.release();
          continue;
        }
        try {
          tasks.submit(() -> {
            try {
              Collection<InetSocketAddress> preferredPeers = mergePeerHints(
                  currentAnnouncePeers(infoHash), recoveredPeers.get(infoHash));
              fetchMetadataByPriority(minimumPriority, infoHash, preferredPeers, timeoutSeconds)
                  .ifPresentOrElse(manifest -> completeMetadataSuccess(infoHash, manifest),
                      () -> completeMetadataFailure(infoHash, "no metadata result"));
            } catch (Exception error) {
              completeMetadataFailure(infoHash, error.getMessage());
            } finally {
              if (lease != null) lease.close();
              runningMetadata.remove(infoHash);
              metadataPermits.release();
            }
          });
        } catch (RuntimeException error) {
          if (lease != null) lease.close();
          runningMetadata.remove(infoHash);
          metadataPermits.release();
          throw error;
        }
      }
    } finally {
      if (remaining > 0) metadataPermits.release(remaining);
    }
  }

  private java.util.Optional<Manifest> fetchMetadataByPriority(int priority, String infoHash,
                                                                Collection<InetSocketAddress> peers,
                                                                int timeoutSeconds) throws Exception {
    if (priority >= LIVE_METADATA_PRIORITY) return fetchHotMetadata(infoHash, peers, timeoutSeconds);
    return metadata.fetch(infoHash, peers, timeoutSeconds).toCompletableFuture()
        .get(timeoutSeconds + 5L, TimeUnit.SECONDS);
  }

  /**
   * Public bootstrap routers currently answer with empty node lists. Passive traffic already
   * gives us live DHT endpoints, so probe a bounded number of those endpoints to seed mldht's
   * routing table. addDHTNode performs the library's bogon/type checks and stops accepting nodes
   * once the table is populated.
   */
  private void seedRoutingTable(DHT dht, InetSocketAddress source) {
    if (source == null || source.getAddress() == null || source.getPort() < 1
        || source.getPort() > 65535 || dht.getNode().getNumEntriesInRoutingTable() >= 30) return;
    AtomicLong gate = incomingNodeProbeGates.computeIfAbsent(dht, ignored -> new AtomicLong());
    long now = System.nanoTime();
    while (true) {
      long allowedAt = gate.get();
      if (now < allowedAt) return;
      if (gate.compareAndSet(allowedAt, now + INCOMING_NODE_PROBE_INTERVAL_NANOS)) break;
    }
    dht.addDHTNode(source.getAddress().getHostAddress(), source.getPort());
  }

  private void seedConfiguredBootstrap(DHT dht) {
    String configured = System.getenv("DHT_BOOTSTRAP");
    if (configured == null || configured.isBlank()) return;
    for (String value : configured.split(",")) {
      String endpoint = value.trim();
      int separator = endpoint.lastIndexOf(':');
      if (separator <= 0 || separator == endpoint.length() - 1) continue;
      try {
        String host = endpoint.substring(0, separator).trim();
        int port = Integer.parseInt(endpoint.substring(separator + 1).trim());
        InetAddress address = InetAddress.getByName(host);
        if (port > 0 && port <= 65535 && address.getAddress().length == 4) {
          dht.addDHTNode(address.getHostAddress(), port);
        }
      } catch (Exception ignored) {
        System.err.println("DHT bootstrap endpoint unavailable: " + endpoint);
      }
    }
  }

  private void startMetadataForAnnounce(String infoHash) {
    if (!metadataPermits.tryAcquire()) return;
    if (runningDirectMetadata.putIfAbsent(infoHash, Boolean.TRUE) != null) {
      metadataPermits.release();
      return;
    }
    TaskLease lease = TaskLease.tryAcquire(redis, infoHash);
    if (redis != null && lease == null) {
      runningDirectMetadata.remove(infoHash);
      metadataPermits.release();
      return;
    }
    PeerSnapshot peers = currentAnnouncePeerSnapshot(infoHash);
    try {
      if (!catalog.claimImmediateMetadataJob(infoHash, Instant.now())) {
        if (lease != null) lease.close();
        runningDirectMetadata.remove(infoHash);
        metadataPermits.release();
        return;
      }
    } catch (Exception error) {
      if (lease != null) lease.close();
      runningDirectMetadata.remove(infoHash);
      metadataPermits.release();
      monitor.metric("collector.failed", 1);
      System.err.println("metadata announce claim failed: " + error.getMessage());
      return;
    }
    tasks.submit(() -> {
      try {
        int timeoutSeconds = liveDirectTimeoutSeconds();
        fetchHotMetadata(infoHash, peers.addresses(), timeoutSeconds)
            .ifPresentOrElse(manifest -> completeDirectMetadataSuccess(infoHash, manifest),
                () -> completeDirectMetadataMiss(infoHash, "no direct metadata result"));
      } catch (Exception error) {
        completeDirectMetadataMiss(infoHash, error.getMessage());
      } finally {
        if (lease != null) lease.close();
        runningDirectMetadata.remove(infoHash);
        metadataPermits.release();
        if (latestAnnouncePeerSequence(infoHash) > peers.sequence()) startMetadataForAnnounce(infoHash);
      }
    });
  }

  private int liveMetadataTimeoutSeconds() {
    return Math.min(LIVE_METADATA_TIMEOUT_SECONDS, config.metadataTimeoutSeconds());
  }

  private int liveDirectTimeoutSeconds() {
    return Math.min(LIVE_DIRECT_TIMEOUT_SECONDS, config.metadataTimeoutSeconds());
  }

  private java.util.Optional<Manifest> fetchHotMetadata(String infoHash,
                                                         Collection<InetSocketAddress> peers,
                                                         int timeoutSeconds) throws Exception {
    var direct = metadata.fetchDirect(infoHash, peers, timeoutSeconds).toCompletableFuture()
        .get(timeoutSeconds + 5L, TimeUnit.SECONDS);
    // Keep mldht graph fallback disabled for the passive collector. Its lookup
    // tasks outlive request deadlines and quickly saturate the task manager.
    return direct;
  }

  private Collection<InetSocketAddress> currentAnnouncePeers(String infoHash) {
    return currentAnnouncePeerSnapshot(infoHash).addresses();
  }

  static List<InetSocketAddress> mergePeerHints(Collection<InetSocketAddress> live,
                                                 Collection<InetSocketAddress> persisted) {
    LinkedHashSet<InetSocketAddress> merged = new LinkedHashSet<>();
    if (live != null) merged.addAll(live);
    if (persisted != null) merged.addAll(persisted);
    return merged.stream().filter(peer -> peer != null && peer.getAddress() != null
        && peer.getPort() > 0 && peer.getPort() <= 65535)
        .limit(DirectMetadataFetcher.MAX_PEERS_PER_FETCH).toList();
  }

  private PeerSnapshot currentAnnouncePeerSnapshot(String infoHash) {
    long cutoff = System.currentTimeMillis() - ANNOUNCE_PEER_TTL_MS;
    Map<InetSocketAddress, PeerHint> hints = announcedPeers.get(infoHash);
    if (hints == null) return new PeerSnapshot(List.of(), 0);
    List<PeerHint> active = hints.values().stream()
        .filter(hint -> hint.observedAt() >= cutoff)
        .sorted(java.util.Comparator.comparingLong(PeerHint::observedAt).reversed())
        .toList();
    long sequence = active.stream().mapToLong(PeerHint::sequence).max().orElse(0);
    return new PeerSnapshot(active.stream().map(PeerHint::address).toList(), sequence);
  }

  private long latestAnnouncePeerSequence(String infoHash) {
    Map<InetSocketAddress, PeerHint> hints = announcedPeers.get(infoHash);
    return hints == null ? 0 : hints.values().stream().mapToLong(PeerHint::sequence).max().orElse(0);
  }

  private void rememberAnnouncedPeers(String infoHash, List<InetSocketAddress> endpoints) {
    long now = System.currentTimeMillis();
    announcedPeers.compute(infoHash, (ignored, existing) -> {
      Map<InetSocketAddress, PeerHint> peers = existing == null
          ? new LinkedHashMap<>() : new LinkedHashMap<>(existing);
      for (InetSocketAddress endpoint : endpoints) {
        peers.remove(endpoint);
        peers.put(endpoint, new PeerHint(endpoint, now, peerSequence.incrementAndGet()));
      }
      while (peers.size() > MAX_ANNOUNCE_ENDPOINTS) peers.remove(peers.keySet().iterator().next());
      return peers;
    });
  }

  private void pruneAnnouncedPeers(long now) {
    long cutoff = now - ANNOUNCE_PEER_TTL_MS;
    for (var entry : announcedPeers.entrySet()) {
      Map<InetSocketAddress, PeerHint> current = entry.getValue();
      Map<InetSocketAddress, PeerHint> active = new LinkedHashMap<>();
      current.forEach((endpoint, hint) -> {
        if (hint.observedAt() >= cutoff) active.put(endpoint, hint);
      });
      if (active.isEmpty()) announcedPeers.remove(entry.getKey(), current);
      else if (active.size() != current.size()) announcedPeers.replace(entry.getKey(), current, active);
    }
    int excess = announcedPeers.size() - MAX_ANNOUNCED_PEER_HASHES;
    if (excess > 0) {
      announcedPeers.entrySet().stream()
          .sorted(java.util.Comparator.comparingLong(entry -> entry.getValue().values().stream()
              .mapToLong(PeerHint::observedAt).max().orElse(Long.MIN_VALUE)))
          .limit(excess)
          .map(Map.Entry::getKey)
          .forEach(announcedPeers::remove);
    }
  }

  static List<InetSocketAddress> announceEndpoints(InetSocketAddress origin, int advertisedPort) {
    if (origin == null || origin.getAddress() == null || advertisedPort < 1 || advertisedPort > 65535) {
      return List.of();
    }
    InetSocketAddress advertised = new InetSocketAddress(origin.getAddress(), advertisedPort);
    if (origin.getPort() < 1 || origin.getPort() > 65535 || origin.getPort() == advertisedPort) {
      return List.of(advertised);
    }
    // mldht does not expose BEP-5 implied_port, so the UDP source is a bounded fallback.
    return List.of(advertised, new InetSocketAddress(origin.getAddress(), origin.getPort()));
  }

  private boolean completeMetadataSuccess(String infoHash, Manifest manifest) {
    try {
      Catalog.ManifestWrite write = catalog.upsertManifest(manifest);
      monitor.metric("metadata.fetch_completed", 1);
      catalog.event("metadata.fetch_completed", infoHash,
          "{\"event\":\"metadata.fetch_completed\",\"info_hash\":\"" + infoHash
              + "\",\"name\":\"" + jsonEscape(manifest.name()) + "\",\"new_content\":"
              + write.inserted() + "}", "passive");
      if (write.inserted()) monitor.metric("content.indexed", 1);
      catalog.completeMetadataJob(infoHash, true, Instant.now());
      return true;
    } catch (Exception error) {
      monitor.metric("collector.failed", 1);
      System.err.println("metadata persistence failed: " + error.getMessage());
      return false;
    }
  }

  private boolean completeDirectMetadataSuccess(String infoHash, Manifest manifest) {
    monitor.metric("metadata.direct_completed", 1);
    return completeMetadataSuccess(infoHash, manifest);
  }

  private boolean completeMetadataFailure(String infoHash, String message) {
    try {
      monitor.metric("metadata.fetch_failed", 1);
      catalog.event("metadata.fetch_failed", infoHash,
          "{\"event\":\"metadata.fetch_failed\",\"info_hash\":\"" + infoHash
              + "\",\"message\":\"" + jsonEscape(message == null ? "unknown" : message) + "\"}", "passive");
      boolean dead = catalog.completeMetadataJob(infoHash, false, Instant.now(), message);
      if (dead && workQueue != null) workQueue.publishDeadTask(infoHash, message);
      return true;
    } catch (Exception error) {
      monitor.metric("collector.failed", 1);
      System.err.println("metadata failure persistence failed: " + error.getMessage());
      return false;
    }
  }

  private boolean completeDirectMetadataMiss(String infoHash, String message) {
    try {
      catalog.queueMetadataJob(infoHash, Instant.now().plusSeconds(DIRECT_FALLBACK_DELAY_SECONDS),
          DIRECT_FALLBACK_PRIORITY, true);
      monitor.metric("metadata.direct_miss", 1);
      catalog.event("metadata.direct_miss", infoHash,
          "{\"event\":\"metadata.direct_miss\",\"info_hash\":\"" + infoHash
              + "\",\"message\":\"" + jsonEscape(message == null ? "unknown" : message) + "\"}", "passive");
      return true;
    } catch (Exception error) {
      monitor.metric("collector.failed", 1);
      System.err.println("direct metadata retry persistence failed: " + error.getMessage());
      return false;
    }
  }

  private static String jsonEscape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r");
  }

  private static JedisPooled connectRedis(String url) {
    if (url == null || url.isBlank()) return null;
    try {
      JedisPooled client = new JedisPooled(java.net.URI.create(url));
      client.ping();
      return client;
    } catch (RuntimeException error) {
      System.err.println("task lease Redis unavailable; using database claims only: " + error.getMessage());
      return null;
    }
  }

  private void flushQueries() throws Exception {
    for (var entry : queryCounts.entrySet()) {
      long count = entry.getValue().sumThenReset();
      if (count > 0) {
        monitor.metric("dht.query_summary", count, entry.getKey());
        if (workQueue == null) {
          catalog.event("dht.query_summary", null,
              "{\"event\":\"dht.query_summary\",\"query\":\"" + entry.getKey()
                  + "\",\"occurrences\":" + count + ",\"interval_seconds\":30}");
        }
      }
    }
  }

  @Override public void close() {
    if (stopping) return;
    stopping = true;
    nodes.forEach(DHT::stop);
    scheduler.shutdownNow();
    observationQueue.close();
    Thread writer = observationWriter;
    if (writer != null) {
      try { writer.join(5_000); }
      catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
      if (writer.isAlive()) writer.interrupt();
    }
    Thread taskReader = metadataQueueReader;
    if (taskReader != null) {
      taskReader.interrupt();
      try { taskReader.join(2_000); }
      catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
    metadata.close();
    tasks.close();
    try {
      flushTouches();
      flushQueries();
    } catch (Exception error) {
      System.err.println("final catalog flush failed: " + error.getMessage());
    }
    monitor.close();
    if (observationSpool != null) observationSpool.close();
    if (redis != null) redis.close();
  }
}
