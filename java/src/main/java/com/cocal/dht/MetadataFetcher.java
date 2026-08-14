package com.cocal.dht;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import the8472.mldht.FetchTaskPeerHints;
import the8472.mldht.TorrentFetcher;

record Manifest(String infoHash, String variant, String name, long totalSize,
                int fileCount, List<ManifestFile> files, int metadataSize,
                String metadataSha256) {}

record ManifestFile(String path, long size) {}

final class MetadataFetcher implements AutoCloseable {
  static final int MAX_METADATA_BYTES = 2 * 1024 * 1024;
  private static final int DIRECT_ATTEMPT_TIMEOUT_SECONDS = 10;
  private static final int DHT_DIRECT_TIMEOUT_SECONDS = 10;
  private static final int DHT_PEER_COLLECTION_DELAY_MILLIS = 500;
  private final TorrentFetcher fetcher;
  private final List<DHT> dhtNodes;
  private final DirectMetadataFetcher direct = new DirectMetadataFetcher();
  private final int timeoutSeconds;
  private final BiConsumer<String, Long> metric;
  private final ConcurrentHashMap.KeySetView<TorrentFetcher.FetchTask, Boolean> activeTasks = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap.KeySetView<PeerLookupTask, Boolean> activeLookups = ConcurrentHashMap.newKeySet();
  private final AtomicLong lookupCursor = new AtomicLong();

  MetadataFetcher(List<lbms.plugins.mldht.kad.DHT> nodes, int maxConcurrent, int timeoutSeconds) {
    this(nodes, maxConcurrent, timeoutSeconds, (ignored, value) -> { });
  }

  MetadataFetcher(List<lbms.plugins.mldht.kad.DHT> nodes, int maxConcurrent, int timeoutSeconds,
                  BiConsumer<String, Long> metric) {
    dhtNodes = List.copyOf(nodes);
    // TorrentFetcher only starts when its RPC server has no queued tasks. Node zero is kept
    // exclusively for it; independent peer lookups rotate over the remaining identities.
    fetcher = new TorrentFetcher(dhtNodes.stream().limit(1).toList());
    fetcher.setMaxOpen(Math.max(1, maxConcurrent));
    fetcher.setMaxSockets(Math.max(8, maxConcurrent * 4));
    this.timeoutSeconds = timeoutSeconds;
    this.metric = metric == null ? (ignored, value) -> { } : metric;
  }

  CompletionStage<Optional<Manifest>> fetch(String infoHash) {
    return fetch(infoHash, (Collection<InetSocketAddress>) null, timeoutSeconds);
  }

  CompletionStage<Optional<Manifest>> fetch(String infoHash, InetSocketAddress preferredPeer) {
    return fetch(infoHash, preferredPeer == null ? List.of() : List.of(preferredPeer), timeoutSeconds);
  }

  CompletionStage<Optional<Manifest>> fetch(String infoHash, Collection<InetSocketAddress> preferredPeers) {
    return fetch(infoHash, preferredPeers, timeoutSeconds);
  }

  CompletionStage<Optional<Manifest>> fetch(String infoHash, Collection<InetSocketAddress> preferredPeers,
                                             int taskTimeoutSeconds) {
    if (taskTimeoutSeconds < 1) throw new IllegalArgumentException("metadata timeout must be positive");
    if (preferredPeers != null && !preferredPeers.isEmpty()) {
      return fetchDirectThenDht(infoHash, preferredPeers, taskTimeoutSeconds);
    }
    return fetchWithDht(infoHash, List.of(), taskTimeoutSeconds);
  }

  CompletionStage<Optional<Manifest>> fetchDirect(String infoHash,
                                                   Collection<InetSocketAddress> preferredPeers,
                                                   int taskTimeoutSeconds) {
    if (taskTimeoutSeconds < 1) throw new IllegalArgumentException("metadata timeout must be positive");
    if (preferredPeers == null || preferredPeers.isEmpty()) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    CompletableFuture<Optional<Manifest>> result = new CompletableFuture<>();
    direct.fetch(infoHash, preferredPeers, taskTimeoutSeconds).whenComplete((raw, error) -> {
      if (error != null) {
        result.completeExceptionally(error);
        return;
      }
      try {
        result.complete(raw.map(bytes -> parse(ByteBuffer.wrap(bytes), infoHash)));
      } catch (Exception parseError) {
        result.completeExceptionally(parseError);
      }
    });
    return result;
  }

  private CompletionStage<Optional<Manifest>> fetchDirectThenDht(String infoHash,
                                                                  Collection<InetSocketAddress> preferredPeers,
                                                                  int taskTimeoutSeconds) {
    CompletableFuture<Optional<Manifest>> result = new CompletableFuture<>();
    long started = System.nanoTime();
    int directTimeout = Math.min(DIRECT_ATTEMPT_TIMEOUT_SECONDS, Math.max(1, taskTimeoutSeconds - 1));
    direct.fetch(infoHash, preferredPeers, directTimeout).whenComplete((raw, directError) -> {
      if (raw != null && raw.isPresent()) {
        try {
          result.complete(Optional.of(parse(java.nio.ByteBuffer.wrap(raw.get()), infoHash)));
          return;
        } catch (Exception ignored) {
          // A malformed direct response should not prevent a DHT fallback.
        }
      }
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      long remainingMillis = Math.max(1_000L, taskTimeoutSeconds * 1_000L - elapsedMillis);
      int remainingSeconds = (int) Math.min(Integer.MAX_VALUE, (remainingMillis + 999L) / 1_000L);
      fetchWithDht(infoHash, preferredPeers, remainingSeconds).whenComplete((fallback, error) -> {
        if (error != null) result.completeExceptionally(error);
        else result.complete(fallback);
      });
    });
    return result;
  }

  private CompletionStage<Optional<Manifest>> fetchWithDht(String infoHash,
                                                            Collection<InetSocketAddress> preferredPeers,
                                                            int taskTimeoutSeconds) {
    Key key = new Key(infoHash);
    metric.accept("metadata.dht_fetch_started", 1L);
    CompletableFuture<Optional<Manifest>> result = new CompletableFuture<>();
    Set<InetSocketAddress> dhtPeers = ConcurrentHashMap.newKeySet();
    List<InetSocketAddress> preferredPeerList = preferredPeers == null ? List.of()
        : preferredPeers.stream().filter(java.util.Objects::nonNull).distinct().toList();
    Set<InetSocketAddress> preferredPeerSet = Set.copyOf(preferredPeerList);
    dhtPeers.addAll(preferredPeerList);
    Set<InetSocketAddress> newDhtPeers = ConcurrentHashMap.newKeySet();
    ConcurrentLinkedQueue<InetSocketAddress> dhtPeerOrder = new ConcurrentLinkedQueue<>();
    AtomicBoolean directStarted = new AtomicBoolean();
    AtomicBoolean directDone = new AtomicBoolean();
    AtomicBoolean directLaunchScheduled = new AtomicBoolean();
    AtomicBoolean dhtDone = new AtomicBoolean();
    AtomicBoolean lookupDone = new AtomicBoolean();
    AtomicReference<Optional<Manifest>> dhtResult = new AtomicReference<>(Optional.empty());
    AtomicReference<Throwable> dhtError = new AtomicReference<>();
    AtomicReference<TorrentFetcher.FetchTask> taskRef = new AtomicReference<>();
    AtomicReference<PeerLookupTask> lookupRef = new AtomicReference<>();
    long startNanos = System.nanoTime();
    Runnable completeDht = () -> {
      if (result.isDone()) return;
      Throwable error = dhtError.get();
      if (error != null) result.completeExceptionally(error);
      else result.complete(dhtResult.get());
    };
    Runnable launchDirect = () -> {
      if (dhtPeers.isEmpty() || !directStarted.compareAndSet(false, true)) return;
      metric.accept("metadata.dht_peers", (long) dhtPeers.size());
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
          - startNanos);
      long remainingMillis = taskTimeoutSeconds * 1_000L - elapsedMillis;
      if (remainingMillis < 1_000L) {
        directDone.set(true);
        if (dhtDone.get() && lookupDone.get()) completeDht.run();
        return;
      }
      int directTimeout = (int) Math.min(DHT_DIRECT_TIMEOUT_SECONDS,
          Math.max(1L, (remainingMillis + 999L) / 1_000L));
      metric.accept("metadata.dht_direct_started", 1L);
      List<InetSocketAddress> candidates = new ArrayList<>(preferredPeerList);
      dhtPeerOrder.forEach(peer -> { if (!candidates.contains(peer)) candidates.add(peer); });
      dhtPeers.forEach(peer -> { if (!candidates.contains(peer)) candidates.add(peer); });
      direct.fetch(infoHash, candidates, directTimeout).whenComplete((raw, error) -> {
        Optional<Manifest> manifest = Optional.empty();
        if (error == null && raw != null && raw.isPresent()) {
          try { manifest = Optional.of(parse(ByteBuffer.wrap(raw.get()), infoHash)); }
          catch (Exception ignored) { }
        }
        directDone.set(true);
        if (manifest.isPresent()) {
          metric.accept("metadata.dht_direct_completed", 1L);
          result.complete(manifest);
          TorrentFetcher.FetchTask task = taskRef.get();
          if (task != null) task.stop();
          PeerLookupTask lookup = lookupRef.get();
          if (lookup != null) lookup.kill();
        } else if (dhtDone.get() && lookupDone.get()) {
          metric.accept("metadata.dht_direct_miss", 1L);
          completeDht.run();
        } else {
          metric.accept("metadata.dht_direct_miss", 1L);
        }
      });
    };
    Runnable scheduleDirect = () -> {
      if (directStarted.get() || result.isDone()
          || !directLaunchScheduled.compareAndSet(false, true)) return;
      CompletableFuture.delayedExecutor(DHT_PEER_COLLECTION_DELAY_MILLIS,
          TimeUnit.MILLISECONDS).execute(() -> {
            directLaunchScheduled.set(false);
            launchDirect.run();
          });
    };
    var task = preferredPeerSet.isEmpty() ? fetcher.fetch(key)
        : fetcher.fetch(key, fetchTask -> preferredPeerSet.forEach(peer -> FetchTaskPeerHints.add(fetchTask, peer)));
    taskRef.set(task);
    activeTasks.add(task);
    PeerLookupTask lookup = startPeerLookup(infoHash, peer -> {
      if (peer == null || !dhtPeers.add(peer)) return;
      if (preferredPeerSet.contains(peer)) return;
      if (!newDhtPeers.add(peer)) return;
      dhtPeerOrder.add(peer);
      int peerCount = newDhtPeers.size();
      if (peerCount >= DirectMetadataFetcher.MAX_PEERS_PER_FETCH) {
        PeerLookupTask activeLookup = lookupRef.get();
        if (activeLookup != null) activeLookup.kill();
        launchDirect.run();
      } else if (preferredPeerSet.isEmpty() ? peerCount >= 3 : peerCount >= 1) {
        scheduleDirect.run();
      }
    }, () -> {
      lookupDone.set(true);
      if (!newDhtPeers.isEmpty() || preferredPeerSet.isEmpty() && !dhtPeers.isEmpty()) {
        launchDirect.run();
      }
      if (dhtDone.get() && (!directStarted.get() || directDone.get())) completeDht.run();
    });
    lookupRef.set(lookup);
    if (lookup == null) lookupDone.set(true);
    task.awaitCompletion().whenComplete((done, error) -> {
      activeTasks.remove(task);
      if (error != null) {
        dhtError.set(error);
      } else {
        try {
          dhtResult.set(done.getResult().map(buffer -> parse(buffer, infoHash)));
          if (dhtResult.get().isPresent()) metric.accept("metadata.dht_library_completed", 1L);
        }
        catch (Exception parseError) { dhtError.set(parseError); }
      }
      dhtDone.set(true);
      if (dhtResult.get().isPresent()) {
        result.complete(dhtResult.get());
        if (lookup != null) lookup.kill();
      } else if (lookupDone.get() && (!directStarted.get() || directDone.get())) {
        completeDht.run();
      }
    });
    CompletableFuture.delayedExecutor(taskTimeoutSeconds, TimeUnit.SECONDS).execute(() -> {
      if (result.isDone()) return;
      dhtDone.set(true);
      lookupDone.set(true);
      task.stop();
      PeerLookupTask activeLookup = lookupRef.get();
      if (activeLookup != null) activeLookup.kill();
      if (!directStarted.get() || directDone.get()) completeDht.run();
    });
    return result;
  }

  private PeerLookupTask startPeerLookup(String infoHash, Consumer<InetSocketAddress> onPeer,
                                         Runnable onComplete) {
    for (int nodeIndex : lookupNodeOrder(dhtNodes.size(), lookupCursor.getAndIncrement())) {
      DHT dht = dhtNodes.get(nodeIndex);
      RPCServer server = dht.getServerManager().getRandomActiveServer(true);
      if (server == null) continue;
      PeerLookupTask lookup = new PeerLookupTask(server, dht.getNode(), new Key(infoHash));
      lookup.setNoAnnounce(true);
      lookup.setFastTerminate(false);
      lookup.setResultHandler((source, item) -> {
        if (item != null) onPeer.accept(item.toSocketAddress());
      });
      activeLookups.add(lookup);
      lookup.addListener(ignored -> {
        activeLookups.remove(lookup);
        onComplete.run();
      });
      metric.accept("metadata.dht_lookup_configured", 1L);
      dht.getTaskManager().addTask(lookup);
      return lookup;
    }
    metric.accept("metadata.dht_lookup_unavailable", 1L);
    return null;
  }

  static List<Integer> lookupNodeOrder(int nodeCount, long cursor) {
    if (nodeCount < 1) return List.of();
    int firstLookupNode = nodeCount > 1 ? 1 : 0;
    int available = nodeCount - firstLookupNode;
    int start = (int) Math.floorMod(cursor, (long) available);
    List<Integer> order = new ArrayList<>(available);
    for (int offset = 0; offset < available; offset++) {
      order.add(firstLookupNode + (start + offset) % available);
    }
    return List.copyOf(order);
  }

  long queuedDhtTasks() {
    return dhtNodes.stream().mapToLong(dht -> dht.getTaskManager().getNumQueuedTasks()).sum();
  }

  long activeDhtTasks() {
    return dhtNodes.stream().mapToLong(dht -> dht.getTaskManager().getActiveTasks().length).sum();
  }

  static Manifest parse(ByteBuffer input, String expectedHash) {
    ByteBuffer copy = input.duplicate();
    byte[] metadata = new byte[copy.remaining()];
    copy.get(metadata);
    if (metadata.length == 0 || metadata.length > MAX_METADATA_BYTES) {
      throw new IllegalArgumentException("metadata size must be between 1 and " + MAX_METADATA_BYTES + " bytes");
    }
    String actualHash = hex(sha1(metadata));
    if (!actualHash.equalsIgnoreCase(expectedHash)) {
      throw new IllegalArgumentException("metadata hash mismatch: expected " + expectedHash + ", got " + actualHash);
    }
    Map<String, Object> info = Bencode.dict(Bencode.decode(metadata));
    String name = text(info.get("name.utf-8"));
    if (name.isBlank()) name = text(info.get("name"));
    List<ManifestFile> files = new ArrayList<>();
    long total = 0;
    int fileCount = 0;
    Object rawFiles = info.get("files");
    if (rawFiles instanceof List<?> list) {
      for (Object rawFile : list) {
        Map<String, Object> file = Bencode.dict(rawFile);
        Object rawPath = file.get("path.utf-8");
        if (rawPath == null) rawPath = file.get("path");
        String path = pathText(rawPath);
        long size = number(file.get("length"));
        if (path.isBlank() || size < 0) throw new IllegalArgumentException("metadata contains an invalid file list");
        fileCount++;
        total = Math.addExact(total, size);
        if (files.size() < 10_000) files.add(new ManifestFile(path, size));
      }
    } else {
      long size = number(info.get("length"));
      if (name.isBlank() || size < 0) throw new IllegalArgumentException("metadata contains an invalid file list");
      files.add(new ManifestFile(name, size));
      fileCount = 1;
      total = size;
    }
    if (files.isEmpty()) throw new IllegalArgumentException("metadata contains an empty file list");
    String variant = info.containsKey("meta version") ? "v2-or-hybrid" : "v1";
    return new Manifest(expectedHash.toLowerCase(Locale.ROOT), variant, name, total,
        fileCount, List.copyOf(files), metadata.length, hex(sha256(metadata)));
  }

  static Manifest parseTorrent(byte[] torrent) {
    Map<String,Object> metainfo;
    try { metainfo = Bencode.dict(Bencode.decode(torrent)); }
    catch (Exception error) { throw new IllegalArgumentException("invalid torrent metainfo: " + error.getMessage(), error); }
    Object info = metainfo.get("info");
    if (!(info instanceof Map<?,?>)) throw new IllegalArgumentException("torrent metainfo is missing an info dictionary");
    byte[] infoBytes = Bencode.encode(info);
    String infoHash = hex(sha1(infoBytes));
    return parse(ByteBuffer.wrap(infoBytes), infoHash);
  }

  private static long number(Object value) {
    return value instanceof Number number ? number.longValue() : -1;
  }

  private static String pathText(Object value) {
    if (value instanceof List<?> list) {
      return list.stream().map(MetadataFetcher::text).filter(part -> !part.isBlank()).reduce((a, b) -> a + "/" + b).orElse("");
    }
    return text(value);
  }

  private static String text(Object value) {
    if (!(value instanceof byte[] bytes)) return value == null ? "" : String.valueOf(value).trim();
    String utf8 = new String(bytes, StandardCharsets.UTF_8).replaceAll("\\p{Cntrl}", "").trim();
    return utf8.indexOf('\ufffd') >= 0 ? new String(bytes, StandardCharsets.ISO_8859_1).replaceAll("\\p{Cntrl}", "").trim() : utf8;
  }

  private static byte[] sha1(byte[] bytes) { return digest("SHA-1", bytes); }
  private static byte[] sha256(byte[] bytes) { return digest("SHA-256", bytes); }
  private static byte[] digest(String algorithm, byte[] bytes) {
    try { return MessageDigest.getInstance(algorithm).digest(bytes); }
    catch (Exception error) { throw new IllegalStateException(error); }
  }
  private static String hex(byte[] bytes) {
    var out = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) out.append(String.format(Locale.ROOT, "%02x", value & 0xff));
    return out.toString();
  }

  @Override public void close() {
    direct.close();
    List.copyOf(activeTasks).forEach(TorrentFetcher.FetchTask::stop);
    List.copyOf(activeLookups).forEach(PeerLookupTask::kill);
  }
}
