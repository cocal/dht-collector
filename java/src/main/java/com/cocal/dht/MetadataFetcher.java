package com.cocal.dht;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lbms.plugins.mldht.kad.Key;
import the8472.mldht.TorrentFetcher;

record Manifest(String infoHash, String variant, String name, long totalSize,
                int fileCount, List<ManifestFile> files, int metadataSize,
                String metadataSha256) {}

record ManifestFile(String path, long size) {}

final class MetadataFetcher implements AutoCloseable {
  static final int MAX_METADATA_BYTES = 2 * 1024 * 1024;
  private final TorrentFetcher fetcher;
  private final int timeoutSeconds;
  private final ConcurrentHashMap.KeySetView<TorrentFetcher.FetchTask, Boolean> activeTasks = ConcurrentHashMap.newKeySet();

  MetadataFetcher(List<lbms.plugins.mldht.kad.DHT> nodes, int maxConcurrent, int timeoutSeconds) {
    fetcher = new TorrentFetcher(nodes);
    fetcher.setMaxOpen(Math.max(1, maxConcurrent));
    fetcher.setMaxSockets(Math.max(8, maxConcurrent * 4));
    this.timeoutSeconds = timeoutSeconds;
  }

  CompletionStage<Optional<Manifest>> fetch(String infoHash) {
    Key key = new Key(infoHash);
    var task = fetcher.fetch(key);
    activeTasks.add(task);
    CompletableFuture<Optional<Manifest>> result = new CompletableFuture<>();
    task.awaitCompletion().whenComplete((done, error) -> {
      activeTasks.remove(task);
      if (error != null) result.completeExceptionally(error);
      else {
        try { result.complete(done.getResult().map(buffer -> parse(buffer, infoHash))); }
        catch (Exception parseError) { result.completeExceptionally(parseError); }
      }
    });
    CompletableFuture.delayedExecutor(timeoutSeconds, TimeUnit.SECONDS).execute(() -> {
      if (!result.isDone()) task.stop();
    });
    return result;
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

  @Override public void close() { activeTasks.forEach(TorrentFetcher.FetchTask::stop); }
}
