package com.cocal.dht;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class EventWriter {
  private final Catalog catalog;
  private final Path output;
  private final ObjectMapper mapper = new ObjectMapper();

  EventWriter(Catalog catalog, Path output) throws IOException {
    this.catalog = catalog;
    this.output = output == null ? null : output.toAbsolutePath().normalize();
    if (this.output != null && this.output.getParent() != null) Files.createDirectories(this.output.getParent());
  }

  Map<String,Object> emit(String type, Map<String,Object> payload) throws Exception {
    Map<String,Object> event = new LinkedHashMap<>();
    event.put("event_id", UUID.randomUUID().toString());
    event.put("schema_version", 1);
    event.put("event", type);
    event.put("occurred_at", Instant.now().toString());
    event.putAll(payload);
    write(event);
    return event;
  }

  Map<String,Object> emitManifest(String type, Map<String,Object> payload, Manifest manifest) throws Exception {
    Map<String,Object> withManifest = new LinkedHashMap<>(payload);
    withManifest.put("manifest", manifestMap(manifest));
    Map<String,Object> event = emit(type, withManifest);
    if (catalog != null) catalog.upsertManifest(manifest);
    return event;
  }

  void ingest(Map<String,Object> event) throws Exception {
    write(event);
    Object rawManifest = event.get("manifest");
    if (catalog != null && rawManifest instanceof Map<?,?> manifest
        && List.of("metadata.fetch_completed", "metadata.import_completed").contains(String.valueOf(event.get("event")))) {
      catalog.upsertManifest(manifestFromMap(manifest));
    }
  }

  private void write(Map<String,Object> event) throws Exception {
    String line = mapper.writeValueAsString(event);
    if (catalog != null) catalog.ingestEvent(event, compactJson(event, line));
    System.out.println(line);
    if (output != null) Files.writeString(output, line + System.lineSeparator(), StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  private String compactJson(Map<String,Object> event, String fullJson) throws IOException {
    Object rawManifest = event.get("manifest");
    if (!(rawManifest instanceof Map<?,?> manifest) || !manifest.containsKey("files")) return fullJson;
    Map<String,Object> compact = new LinkedHashMap<>(event);
    Map<String,Object> compactManifest = new LinkedHashMap<>();
    manifest.forEach((key, value) -> { if (!String.valueOf(key).equals("files")) compactManifest.put(String.valueOf(key), value); });
    compact.put("manifest", compactManifest);
    return mapper.writeValueAsString(compact);
  }

  static Map<String,Object> manifestMap(Manifest manifest) {
    Map<String,Object> result = new LinkedHashMap<>();
    result.put("info_hash", manifest.infoHash());
    result.put("variant", manifest.variant());
    result.put("name", manifest.name());
    result.put("total_size", manifest.totalSize());
    result.put("file_count", manifest.fileCount());
    result.put("files", manifest.files().stream().map(file -> Map.of("path", file.path(), "size", file.size())).toList());
    result.put("metadata_size", manifest.metadataSize());
    result.put("metadata_sha256", manifest.metadataSha256());
    return result;
  }

  static Manifest manifestFromMap(Map<?,?> value) {
    String hash = text(value.get("info_hash"));
    String variant = text(value.get("variant"));
    String name = text(value.get("name"));
    long total = number(value.get("total_size"));
    int count = Math.toIntExact(number(value.get("file_count")));
    int metadataSize = Math.toIntExact(number(value.get("metadata_size")));
    String sha256 = text(value.get("metadata_sha256"));
    java.util.ArrayList<ManifestFile> files = new java.util.ArrayList<>();
    if (value.get("files") instanceof List<?> list) for (Object item : list) {
      if (item instanceof Map<?,?> file) files.add(new ManifestFile(text(file.get("path")), number(file.get("size"))));
    }
    return new Manifest(hash, variant.isBlank() ? "v1" : variant, name, total,
        count == 0 ? files.size() : count, List.copyOf(files), metadataSize, sha256);
  }

  private static String text(Object value) { return value == null ? "" : String.valueOf(value); }
  private static long number(Object value) {
    if (value instanceof Number number) return number.longValue();
    return value == null ? 0 : Long.parseLong(String.valueOf(value));
  }
}
