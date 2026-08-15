package com.cocal.dht;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;

final class DashboardServer implements AutoCloseable {
  private final Catalog catalog;
  private final Config config;
  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpServer server;
  private final JedisPooled redis;

  DashboardServer(Catalog catalog, Config config) throws IOException {
    this.catalog = catalog;
    this.config = config;
    this.redis = connectRedis(config.redisUrl());
    seedLegacyCounters();
    server = HttpServer.create(new InetSocketAddress(config.httpHost(), config.httpPort()), 128);
    server.createContext("/", this::handle);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
  }

  void start() { server.start(); System.out.printf("Java dashboard listening on http://%s:%d%n", config.httpHost(), config.httpPort()); }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/api/health")) { json(exchange, 200, Map.of("ok", true)); return; }
      if (path.equals("/api/system")) { json(exchange, 200, systemSnapshot()); return; }
      if (path.equals("/api/stream")) { stream(exchange); return; }
      if (path.equals("/api/sniffer")) { handleSniffer(exchange); return; }
      if (path.equals("/api/dashboard")) { int limit = bounded(query(exchange, "limit", 80), 1, 200, 80); json(exchange, 200, dashboard(limit)); return; }
      if (path.equals("/api/content")) { page(exchange, null); return; }
      if (path.equals("/api/search")) { page(exchange, query(exchange, "q", "")); return; }
      staticFile(exchange, path);
    } catch (IllegalArgumentException error) {
      json(exchange, 400, Map.of("error", error.getMessage()));
    } catch (Exception error) {
      json(exchange, 500, Map.of("error", error.getMessage() == null ? "internal error" : error.getMessage()));
    } finally { exchange.close(); }
  }

  private Map<String,Object> dashboard(int limit) throws Exception {
    List<Map<String,Object>> buckets = catalog.trend(5);
    Instant to = Instant.now().truncatedTo(ChronoUnit.MINUTES);
    Map<String,Object> trend = new LinkedHashMap<>();
    trend.put("from", to.minus(5, ChronoUnit.MINUTES).toString());
    trend.put("to", to.toString());
    trend.put("bucket_seconds", 60);
    trend.put("buckets", buckets);
    Map<String,Object> result = new LinkedHashMap<>();
    result.put("summary", redisSummary()); result.put("trend", trend);
    result.put("probes", catalog.recentProbes(limit)); result.put("content", catalog.contentPage(limit, 0)); return result;
  }

  private Map<String,Object> redisSummary() throws Exception {
    if (redis == null) return catalog.dashboardSummary();
    Map<String, String> raw = redis.hgetAll("dht:summary");
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("content", number(raw.get("content")));
    result.put("files", number(raw.get("files")));
    result.put("probes", number(raw.get("probes")));
    result.put("peers", number(raw.get("peers")));
    result.put("lookups", number(raw.get("lookups")));
    result.put("discovered", number(raw.get("discovered")));
    result.put("active_discovered", number(raw.get("active_discovered")));
    result.put("invalid_discovered", number(raw.get("invalid_discovered")));
    result.put("last_event_at", raw.get("last_event_at"));
    result.put("redis_up", number(raw.get("redis.up")) > 0);
    result.put("redis_latency_ms", number(raw.get("redis.latency_ms")));
    result.put("redis_used_memory_bytes", number(raw.get("redis.used_memory_bytes")));
    result.put("redis_connected_clients", number(raw.get("redis.connected_clients")));
    return result;
  }

  private Map<String,Object> systemSnapshot() {
    Map<String,Object> result = new LinkedHashMap<>();
    result.put("services", serviceStatuses());
    result.put("host", hostSnapshot());
    result.put("redis", redisSnapshot());
    result.put("nodes", nodeSnapshots());
    return result;
  }

  private Map<String,Object> nodeSnapshots() {
    if (redis == null) return Map.of();
    try {
      Set<String> ids = new java.util.TreeSet<>();
      for (String key : redis.keys("dht:node:*") ) {
        String suffix = key.substring("dht:node:".length());
        ids.add(suffix.endsWith(":heartbeat") ? suffix.substring(0, suffix.length() - ":heartbeat".length()) : suffix);
      }
      Map<String,Object> result = new LinkedHashMap<>();
      for (String id : ids) {
        Map<String,String> raw = redis.hgetAll("dht:node:" + id);
        Map<String,Object> node = new LinkedHashMap<>();
        node.put("completed", number(raw.get("metadata.fetch_completed")));
        node.put("failed", number(raw.get("metadata.fetch_failed")));
        node.put("indexed", number(raw.get("content.indexed")));
        node.put("last_event_at", raw.get("last_event_at"));
        node.put("heartbeat_ttl", redis.ttl("dht:node:" + id + ":heartbeat"));
        result.put(id, node);
      }
      return result;
    } catch (Exception error) { return Map.of(); }
  }

  private Map<String,String> serviceStatuses() {
    Map<String,String> result = new LinkedHashMap<>();
    for (String service : List.of("dht-passive-collector.service", "dht-monitor-redis-bridge.service",
        "keydb.service", "dht-search-dashboard.service")) {
      try {
        Process process = new ProcessBuilder("systemctl", "is-active", service).start();
        String state = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        process.waitFor();
        result.put(service, state.isBlank() ? "unknown" : state);
      } catch (Exception error) { result.put(service, "unknown"); }
    }
    try (var connection = catalog.connection(); var statement = connection.createStatement()) {
      statement.execute("SELECT 1");
      result.put("postgresql", "active");
    } catch (Exception error) { result.put("postgresql", "inactive"); }
    return result;
  }

  private Map<String,Object> hostSnapshot() {
    Map<String,Object> result = new LinkedHashMap<>();
    try {
      Map<String,Object> memory = new LinkedHashMap<>();
      for (String line : Files.readAllLines(Path.of("/proc/meminfo"))) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 2 && Set.of("MemTotal:", "MemAvailable:", "SwapTotal:", "SwapFree:").contains(parts[0])) {
          memory.put(parts[0].replace(":", "").toLowerCase(), Long.parseLong(parts[1]) * 1024L);
        }
      }
      result.put("memory", memory);
    } catch (Exception error) { result.put("memory", Map.of()); }
    try {
      var store = Files.getFileStore(Path.of("/"));
      result.put("disk", Map.of("total", store.getTotalSpace(), "free", store.getUsableSpace()));
    } catch (Exception error) { result.put("disk", Map.of()); }
    return result;
  }

  private Map<String,Object> redisSnapshot() {
    if (redis == null) return Map.of("available", false);
    try {
      String infoText;
      String clientList;
      try (Jedis client = new Jedis(java.net.URI.create(config.redisUrl()))) {
        infoText = client.info();
        clientList = client.clientList();
      }
      Map<String,String> info = parseInfo(infoText);
      Map<String,Object> result = new LinkedHashMap<>();
      result.put("available", true); result.put("version", info.getOrDefault("redis_version", "unknown"));
      result.put("uptime_seconds", number(info.get("uptime_in_seconds")));
      result.put("used_memory_bytes", number(info.get("used_memory")));
      result.put("peak_memory_bytes", number(info.get("used_memory_peak")));
      result.put("connected_clients", number(info.get("connected_clients")));
      result.put("blocked_clients", number(info.get("blocked_clients")));
      result.put("keys", redis.dbSize()); result.put("summary_fields", redis.hlen("dht:summary"));
      result.put("client_list_entries", clientList.isBlank() ? 0 : clientList.lines().count());
      return result;
    } catch (Exception error) { return Map.of("available", false, "error", error.getMessage()); }
  }

  private static Map<String,String> parseInfo(String text) {
    Map<String,String> values = new LinkedHashMap<>();
    for (String line : text.split("\\R")) {
      int split = line.indexOf(':');
      if (split > 0) values.put(line.substring(0, split), line.substring(split + 1));
    }
    return values;
  }

  private void seedLegacyCounters() {
    if (redis == null) return;
    try {
      for (String name : new String[]{"content", "files", "probes", "peers", "lookups", "discovered"}) {
        long catalogValue = catalog.counterValue(name);
        String current = redis.hget("dht:summary", name);
        if (current == null || number(current) < catalogValue) {
          redis.hset("dht:summary", name, Long.toString(catalogValue));
        }
      }
    } catch (Exception ignored) { }
  }

  private void stream(HttpExchange exchange) throws IOException {
    if (redis == null) { json(exchange, 503, Map.of("error", "REDIS_URL is not configured")); return; }
    exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
    exchange.getResponseHeaders().set("Cache-Control", "no-cache");
    exchange.getResponseHeaders().set("Connection", "keep-alive");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, 0);
    try (var output = exchange.getResponseBody(); Jedis subscriber = new Jedis(java.net.URI.create(config.redisUrl()))) {
      writeSse(output, Map.of("summary", redisSummary()));
      subscriber.subscribe(new JedisPubSub() {
        @Override public void onMessage(String channel, String message) {
          try { writeSse(output, Map.of("event", mapper.readValue(message, Map.class), "summary", redisSummary())); }
          catch (Exception error) { unsubscribe(); }
        }
      }, "dht:summary:update");
    } catch (Exception ignored) {
      // Client disconnects are normal for SSE connections.
    }
  }

  private static void writeSse(java.io.OutputStream output, Object value) throws IOException {
    output.write(("data: " + new ObjectMapper().writeValueAsString(value) + "\n\n").getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  private static long number(String value) {
    try { return value == null ? 0 : Long.parseLong(value); }
    catch (NumberFormatException ignored) { return 0; }
  }

  private static JedisPooled connectRedis(String url) {
    if (url == null || url.isBlank()) return null;
    try { JedisPooled client = new JedisPooled(java.net.URI.create(url)); client.ping(); return client; }
    catch (Exception ignored) { return null; }
  }

  private void page(HttpExchange exchange, String search) throws Exception {
    int page = bounded(query(exchange, "page", 1), 1, Integer.MAX_VALUE, 1);
    int pageSize = bounded(query(exchange, "page_size", 20), 1, 100, 20);
    boolean emptySearch = search != null && search.isBlank();
    List<Map<String,Object>> results = emptySearch ? List.of()
        : search == null ? catalog.contentPage(pageSize, (page - 1) * pageSize)
        : catalog.searchPage(search, pageSize, (page - 1) * pageSize);
    long total = emptySearch ? 0 : catalog.contentCount(search);
    json(exchange, 200, Map.of("query", search == null ? "" : search, "page", page, "page_size", pageSize, "total", total, "total_pages", (total + pageSize - 1) / pageSize, "results", results));
  }

  private void handleSniffer(HttpExchange exchange) throws Exception {
    if (exchange.getRequestMethod().equals("GET")) { json(exchange, 200, snifferStatus()); return; }
    if (!exchange.getRequestMethod().equals("POST")) { json(exchange, 405, Map.of("error", "method not allowed")); return; }
    if (exchange.getRequestHeaders().getFirst("Content-Length") != null
        && Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length")) > 16 * 1024) {
      json(exchange, 413, Map.of("error", "request body too large")); return;
    }
    byte[] bodyBytes = exchange.getRequestBody().readNBytes(16 * 1024 + 1);
    if (bodyBytes.length > 16 * 1024) { json(exchange, 413, Map.of("error", "request body too large")); return; }
    String body = new String(bodyBytes, StandardCharsets.UTF_8);
    Map<?,?> parsed = mapper.readValue(body.isBlank() ? "{}" : body, Map.class);
    Object enabled = parsed.get("enabled");
    if (!(enabled instanceof Boolean value)) { json(exchange, 400, Map.of("error", "enabled must be boolean")); return; }
    int exit = new ProcessBuilder("/usr/bin/systemctl", value ? "start" : "stop", "dht-passive-collector.service").inheritIO().start().waitFor();
    if (exit != 0) throw new IOException("systemctl returned " + exit);
    json(exchange, 200, snifferStatus());
  }

  private Map<String,Object> snifferStatus() throws Exception {
    Process process = new ProcessBuilder("/usr/bin/systemctl", "is-active", "dht-passive-collector.service").start();
    String status = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim(); process.waitFor();
    if (status.isBlank()) status = "unknown";
    return Map.of("service", "dht-passive-collector.service", "status", status, "enabled", status.equals("active"));
  }

  private void staticFile(HttpExchange exchange, String requestPath) throws IOException {
    String relative = requestPath.equals("/") ? "index.html" : requestPath.substring(1);
    Path root = config.staticPath().toAbsolutePath().normalize(); Path file = root.resolve(relative).normalize();
    if (!file.startsWith(root)) { json(exchange, 403, Map.of("error", "forbidden")); return; }
    if (!Files.isRegularFile(file)) { json(exchange, 404, Map.of("error", "not found")); return; }
    String name = file.getFileName().toString();
    int extensionAt = name.lastIndexOf('.');
    String extension = extensionAt < 0 ? "" : name.substring(extensionAt + 1);
    String contentType = switch (extension) { case "html" -> "text/html; charset=utf-8"; case "js" -> "text/javascript; charset=utf-8"; case "css" -> "text/css; charset=utf-8"; case "json" -> "application/json; charset=utf-8"; case "svg" -> "image/svg+xml"; default -> "application/octet-stream"; };
    byte[] bytes = Files.readAllBytes(file); exchange.getResponseHeaders().set("Content-Type", contentType); exchange.getResponseHeaders().set("Cache-Control", "no-cache"); exchange.sendResponseHeaders(200, bytes.length); exchange.getResponseBody().write(bytes);
  }

  private void json(HttpExchange exchange, int status, Object value) throws IOException { byte[] bytes = mapper.writeValueAsBytes(value); exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8"); exchange.getResponseHeaders().set("Cache-Control", "no-store"); exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*"); exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes); }
  private static String query(HttpExchange exchange, String name, String fallback) { String raw = exchange.getRequestURI().getQuery(); if (raw != null) for (String part : raw.split("&")) { String[] pair = part.split("=",2); if (pair.length == 2 && pair[0].equals(name)) return java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8); } return fallback; }
  private static int query(HttpExchange exchange, String name, int fallback) { try { return Integer.parseInt(query(exchange, name, Integer.toString(fallback))); } catch (Exception ignored) { return fallback; } }
  private static int bounded(int value, int min, int max, int fallback) { return value < min || value > max ? fallback : value; }
  @Override public void close() { server.stop(1); if (redis != null) redis.close(); }
}
