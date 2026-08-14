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
import java.util.concurrent.Executors;

final class DashboardServer implements AutoCloseable {
  private final Catalog catalog;
  private final Config config;
  private final ObjectMapper mapper = new ObjectMapper();
  private final HttpServer server;

  DashboardServer(Catalog catalog, Config config) throws IOException {
    this.catalog = catalog;
    this.config = config;
    server = HttpServer.create(new InetSocketAddress(config.httpHost(), config.httpPort()), 128);
    server.createContext("/", this::handle);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
  }

  void start() { server.start(); System.out.printf("Java dashboard listening on http://%s:%d%n", config.httpHost(), config.httpPort()); }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/api/health")) { json(exchange, 200, Map.of("ok", true)); return; }
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
    result.put("summary", catalog.dashboardSummary()); result.put("trend", trend);
    result.put("probes", catalog.recentProbes(limit)); result.put("content", catalog.contentPage(limit, 0)); return result;
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
  @Override public void close() { server.stop(1); }
}
