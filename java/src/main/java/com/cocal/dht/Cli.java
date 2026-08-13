package com.cocal.dht;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class Cli {
  record ApprovedTarget(String infoHash, String authorizationRef) {}
  private static final ObjectMapper JSON = new ObjectMapper();
  private Cli() {}

  static boolean supports(String mode) {
    return Set.of("health", "lookup", "metadata-worker", "catalog", "approved-indexer",
        "import-torrent", "migrate-sqlite").contains(mode);
  }

  static void run(String[] rawArgs) throws Exception {
    Args args = Args.parse(rawArgs);
    switch (args.required("mode")) {
      case "health" -> health(args);
      case "lookup" -> lookup(args);
      case "metadata-worker" -> metadataWorker(args);
      case "catalog" -> catalog(args);
      case "approved-indexer" -> approvedIndexer(args);
      case "import-torrent" -> importTorrent(args);
      case "migrate-sqlite" -> SqliteMigrator.run(args, database(args, true));
      default -> throw new IllegalArgumentException("unsupported mode");
    }
  }

  private static void health(Args args) throws Exception {
    try (Catalog catalog = database(args, false);
         DhtRuntime dht = runtime(args, "health")) {
      initialize(catalog);
      EventWriter events = new EventWriter(catalog, args.path("event-log", null));
      dht.start();
      events.emit("collector.ready", map("mode", "health", "listen", dht.status()));
      Thread.sleep(args.positiveLong("duration-ms", 5_000));
      events.emit("collector.snapshot", merge(map("mode", "health"), dht.status()));
    }
  }

  private static void lookup(Args args) throws Exception {
    String infoHash = normalizeHash(args.required("info-hash"));
    try (Catalog catalog = database(args, false);
         DhtRuntime dht = runtime(args, "lookup")) {
      initialize(catalog);
      EventWriter events = new EventWriter(catalog, args.path("event-log", null));
      dht.start();
      events.emit("collector.ready", map("mode", "lookup", "info_hash", infoHash, "listen", dht.status()));
      DhtRuntime.LookupResult result = dht.lookup(infoHash,
          Duration.ofMillis(args.positiveLong("timeout-ms", 30_000)), (peer, source) -> {
            Map<String,Object> payload = map("mode", "lookup", "info_hash", infoHash);
            if (!args.flag("no-peer-address")) {
              payload.put("peer", map("host", peer.getAddress().getHostAddress(), "port", peer.getPort()));
              if (source != null) payload.put("discovered_from", map("host", source.getAddress().getHostAddress(), "port", source.getPort()));
            }
            events.emit("dht.peer_discovered", payload);
          });
      events.emit("dht.lookup_completed", map("mode", "lookup", "info_hash", infoHash,
          "peers", result.peers(), "requests", result.requests(), "responses", result.responses(),
          "elapsed_ms", result.elapsedMillis()));
    }
  }

  private static void metadataWorker(Args args) throws Exception {
    Path input = args.inputPath();
    try (Catalog catalog = database(args, false);
         DhtRuntime dht = runtime(args, "metadata-worker")) {
      initialize(catalog);
      EventWriter events = new EventWriter(catalog, args.path("output", null));
      dht.start();
      try (MetadataFetcher fetcher = new MetadataFetcher(List.of(dht.dht()), 1,
          Math.max(1, Math.toIntExact((args.positiveLong("timeout-ms", 10_000) + 999) / 1_000)));
           BufferedReader lines = reader(input)) {
        events.emit("metadata.worker_ready", map("input", input == null ? "-" : input.toString()));
        Map<String,Integer> attempts = new LinkedHashMap<>();
        int max = args.positiveInt("max-per-infohash", 3);
        for (String line; (line = lines.readLine()) != null;) {
          if (line.isBlank()) continue;
          Map<String,Object> event;
          try { event = JSON.readValue(line, new TypeReference<>() {}); }
          catch (Exception ignored) { continue; }
          if (!String.valueOf(event.get("event")).equals("dht.peer_discovered")) continue;
          String hash;
          try { hash = normalizeHash(String.valueOf(event.get("info_hash"))); }
          catch (Exception ignored) { continue; }
          int attempt = attempts.merge(hash, 1, Integer::sum);
          if (attempt > max) continue;
          events.emit("metadata.fetch_started", map("info_hash", hash, "attempt", attempt));
          try {
            var manifest = fetcher.fetch(hash).toCompletableFuture()
                .get(args.positiveLong("timeout-ms", 10_000) + 5_000, TimeUnit.MILLISECONDS);
            if (manifest.isPresent()) events.emitManifest("metadata.fetch_completed", map("info_hash", hash), manifest.get());
            else events.emit("metadata.fetch_failed", map("info_hash", hash, "message", "no metadata result"));
          } catch (Exception error) {
            events.emit("metadata.fetch_failed", map("info_hash", hash, "message", message(error)));
          }
        }
      }
    }
  }

  private static void catalog(Args args) throws Exception {
    try (Catalog catalog = database(args, true)) {
      catalog.initialize();
      String command = args.value("command", "stats");
      if (command.equals("stats")) System.out.println(JSON.writeValueAsString(catalog.dashboardSummary()));
      else if (command.equals("search")) {
        String query = args.required("query").trim();
        int limit = args.positiveInt("limit", 20);
        System.out.println(JSON.writeValueAsString(map("query", query, "results", catalog.searchPage(query, limit, 0))));
      } else if (command.equals("ingest")) {
        Path input = args.inputPath();
        int accepted = 0;
        try (BufferedReader lines = reader(input)) {
          for (String line; (line = lines.readLine()) != null;) {
            if (line.isBlank()) continue;
            try {
              Map<String,Object> event = JSON.readValue(line, new TypeReference<>() {});
              catalog.ingestEvent(event, line);
              if (event.get("manifest") instanceof Map<?,?> manifest
                  && Set.of("metadata.fetch_completed", "metadata.import_completed").contains(String.valueOf(event.get("event")))) {
                catalog.upsertManifest(EventWriter.manifestFromMap(manifest));
              }
              accepted++;
            } catch (Exception error) { System.err.println("catalog skipped event: " + message(error)); }
          }
        }
        System.out.println(JSON.writeValueAsString(map("accepted", accepted)));
      } else throw new IllegalArgumentException("--command must be stats, search, or ingest");
    }
  }

  private static void importTorrent(Args args) throws Exception {
    String authorization = args.required("authorization-ref").trim();
    if (authorization.isBlank()) throw new IllegalArgumentException("--authorization-ref is required");
    Manifest manifest = MetadataFetcher.parseTorrent(Files.readAllBytes(args.requiredPath("input")));
    try (Catalog catalog = database(args, true)) {
      catalog.initialize();
      EventWriter events = new EventWriter(catalog, args.path("event-log", null));
      events.emitManifest("metadata.import_completed", map("info_hash", manifest.infoHash(),
          "authorization_ref", authorization, "metadata_source", "authorized-torrent-file"), manifest);
    }
  }

  private static void approvedIndexer(Args args) throws Exception {
    List<ApprovedTarget> initial = loadApprovedTargets(args.requiredPath("input"));
    try (Catalog catalog = database(args, true);
         DhtRuntime dht = runtime(args, "approved-indexer")) {
      catalog.initialize();
      EventWriter events = new EventWriter(catalog, args.path("event-log", null));
      dht.start();
      try (MetadataFetcher fetcher = new MetadataFetcher(List.of(dht.dht()),
          args.positiveInt("metadata-concurrent", 3),
          Math.max(1, Math.toIntExact((args.positiveLong("metadata-timeout-ms", 10_000) + 999) / 1_000)))) {
        events.emit("authorized.indexer_ready", map("mode", "approved-index", "listen", dht.status(),
            "approved_targets", initial.size()));
        long interval = args.nonNegativeLong("interval-ms", 0);
        int cycle = 0;
        do {
          List<ApprovedTarget> targets = loadApprovedTargets(args.requiredPath("input"));
          events.emit("authorized.indexer_cycle_started", map("mode", "approved-index", "cycle", ++cycle,
              "approved_targets", targets.size()));
          for (ApprovedTarget target : targets) indexApproved(target, args, dht, fetcher, events);
          events.emit("authorized.indexer_cycle_completed", map("mode", "approved-index", "cycle", cycle,
              "approved_targets", targets.size()));
          if (interval > 0) Thread.sleep(interval);
        } while (interval > 0);
      }
    }
  }

  private static void indexApproved(ApprovedTarget target, Args args, DhtRuntime dht,
                                    MetadataFetcher fetcher, EventWriter events) throws Exception {
    Map<String,Object> base = map("mode", "approved-index", "info_hash", target.infoHash(),
        "authorization_ref", target.authorizationRef());
    events.emit("authorized.lookup_started", base);
    try {
      DhtRuntime.LookupResult result = dht.lookup(target.infoHash(),
          Duration.ofMillis(args.positiveLong("lookup-timeout-ms", 30_000)), (peer, source) -> {
            Map<String,Object> payload = new LinkedHashMap<>(base);
            if (!args.flag("no-peer-address")) payload.put("peer",
                map("host", peer.getAddress().getHostAddress(), "port", peer.getPort()));
            events.emit("dht.peer_discovered", payload);
          });
      events.emit("metadata.fetch_started", base);
      var manifest = fetcher.fetch(target.infoHash()).toCompletableFuture()
          .get(args.positiveLong("metadata-timeout-ms", 10_000) + 5_000, TimeUnit.MILLISECONDS);
      if (manifest.isPresent()) events.emitManifest("metadata.fetch_completed", base, manifest.get());
      else events.emit("metadata.fetch_failed", merge(base, map("message", "no metadata result")));
      events.emit("authorized.lookup_completed", merge(base, map("peers", result.peers(),
          "requests", result.requests(), "responses", result.responses())));
    } catch (Exception error) {
      events.emit("authorized.lookup_failed", merge(base, map("message", message(error))));
    }
  }

  static List<ApprovedTarget> loadApprovedTargets(Path input) throws Exception {
    List<ApprovedTarget> result = new ArrayList<>();
    Set<String> hashes = new LinkedHashSet<>();
    try (BufferedReader lines = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
      int lineNumber = 0;
      for (String line; (line = lines.readLine()) != null;) {
        lineNumber++;
        if (line.isBlank() || line.stripLeading().startsWith("#")) continue;
        Map<String,Object> record;
        try { record = JSON.readValue(line, new TypeReference<>() {}); }
        catch (Exception error) { throw new IllegalArgumentException("line " + lineNumber + ": invalid JSON", error); }
        String hash;
        try { hash = normalizeHash(String.valueOf(record.get("info_hash"))); }
        catch (Exception error) { throw new IllegalArgumentException("line " + lineNumber + ": info_hash must be a 40-character hexadecimal v1 hash"); }
        String authorization = record.get("authorization_ref") == null ? "" : String.valueOf(record.get("authorization_ref")).trim();
        if (authorization.isBlank()) throw new IllegalArgumentException("line " + lineNumber + ": authorization_ref is required");
        if (!hashes.add(hash)) throw new IllegalArgumentException("line " + lineNumber + ": duplicate info_hash");
        result.add(new ApprovedTarget(hash, authorization));
      }
    }
    if (result.isEmpty()) throw new IllegalArgumentException("input has no approved infohash records");
    return List.copyOf(result);
  }

  private static DhtRuntime runtime(Args args, String name) {
    return new DhtRuntime(args.value("address", "0.0.0.0"), args.nonNegativeInt("port", 0),
        args.path("storage-path", Path.of("var", "java-cli", name)));
  }

  private static Catalog database(Args args, boolean required) {
    String url = args.value("db-url", System.getenv("DATABASE_URL"));
    if (url == null || url.isBlank()) {
      if (required) throw new IllegalArgumentException("DATABASE_URL or --db-url is required");
      return null;
    }
    return new Catalog(url, args.value("db-user", System.getenv("PGUSER")),
        args.value("db-password", System.getenv("PGPASSWORD")), args.positiveInt("pool-size", 3));
  }

  private static void initialize(Catalog catalog) throws Exception { if (catalog != null) catalog.initialize(); }
  private static BufferedReader reader(Path input) throws Exception {
    return input == null ? new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
        : Files.newBufferedReader(input, StandardCharsets.UTF_8);
  }
  static String normalizeHash(String value) {
    String hash = value == null ? "" : value.toLowerCase(java.util.Locale.ROOT);
    if (!hash.matches("[a-f0-9]{40}")) throw new IllegalArgumentException("info hash must be 40 hexadecimal characters");
    return hash;
  }
  private static String message(Throwable error) {
    Throwable value = error;
    while (value.getCause() != null) value = value.getCause();
    return value.getMessage() == null ? value.getClass().getSimpleName() : value.getMessage();
  }
  @SafeVarargs private static Map<String,Object> merge(Map<String,Object>... values) {
    Map<String,Object> result = new LinkedHashMap<>(); for (Map<String,Object> value : values) result.putAll(value); return result;
  }
  private static Map<String,Object> map(Object... values) {
    Map<String,Object> result = new LinkedHashMap<>();
    for (int index = 0; index < values.length; index += 2) result.put(String.valueOf(values[index]), values[index + 1]);
    return result;
  }

  static final class Args {
    private static final Set<String> FLAGS = Set.of("no-peer-address", "quiet");
    private final Map<String,String> values = new LinkedHashMap<>();
    private final Set<String> flags = new LinkedHashSet<>();
    static Args parse(String[] args) {
      Args result = new Args();
      for (int index = 0; index < args.length; index++) {
        String raw = args[index];
        if (!raw.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + raw);
        String key = raw.substring(2);
        if (FLAGS.contains(key)) { result.flags.add(key); continue; }
        if (++index >= args.length) throw new IllegalArgumentException(raw + " requires a value");
        result.values.put(key, args[index]);
      }
      return result;
    }
    String value(String name, String fallback) { return values.getOrDefault(name, fallback); }
    String required(String name) { String value = values.get(name); if (value == null || value.isBlank()) throw new IllegalArgumentException("--" + name + " is required"); return value; }
    boolean flag(String name) { return flags.contains(name); }
    Path path(String name, Path fallback) { String value = values.get(name); return value == null ? fallback : Path.of(value); }
    Path requiredPath(String name) { return Path.of(required(name)); }
    Path inputPath() { String value = required("input"); return value.equals("-") ? null : Path.of(value); }
    int positiveInt(String name, int fallback) { long value = positiveLong(name, fallback); if (value > Integer.MAX_VALUE) throw new IllegalArgumentException("--" + name + " is too large"); return (int)value; }
    int nonNegativeInt(String name, int fallback) { long value = nonNegativeLong(name, fallback); if (value > 65535) throw new IllegalArgumentException("--" + name + " must be <= 65535"); return (int)value; }
    long positiveLong(String name, long fallback) { long value = number(name, fallback); if (value <= 0) throw new IllegalArgumentException("--" + name + " must be positive"); return value; }
    long nonNegativeLong(String name, long fallback) { long value = number(name, fallback); if (value < 0) throw new IllegalArgumentException("--" + name + " must not be negative"); return value; }
    private long number(String name, long fallback) { try { return Long.parseLong(values.getOrDefault(name, Long.toString(fallback))); } catch (NumberFormatException error) { throw new IllegalArgumentException("--" + name + " must be an integer", error); } }
  }
}
