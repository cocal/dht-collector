package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class MetadataFetcherTest {
  @Test
  void parsesAndVerifiesSingleFileMetadata() throws Exception {
    byte[] info = Bencode.encode(Map.of("length", 42L, "name", "sample.txt"));
    String hash = hex(MessageDigest.getInstance("SHA-1").digest(info));
    Manifest manifest = MetadataFetcher.parse(ByteBuffer.wrap(info), hash);
    assertEquals(hash, manifest.infoHash());
    assertEquals("sample.txt", manifest.name());
    assertEquals(42, manifest.totalSize());
    assertEquals(List.of(new ManifestFile("sample.txt", 42)), manifest.files());
  }

  @Test
  void rejectsWrongInfoHash() throws Exception {
    byte[] info = Bencode.encode(Map.of("length", 1L, "name", "x"));
    assertThrows(IllegalArgumentException.class, () -> MetadataFetcher.parse(ByteBuffer.wrap(info), "0".repeat(40)));
  }

  @Test
  void parsesAuthorizedTorrentMetainfo() {
    Map<String,Object> info = new LinkedHashMap<>();
    info.put("length", 128L);
    info.put("name", "authorized-release.iso".getBytes(StandardCharsets.UTF_8));
    info.put("piece length", 16_384L);
    info.put("pieces", new byte[20]);
    Map<String,Object> metainfo = new LinkedHashMap<>();
    metainfo.put("announce", "https://tracker.example/announce".getBytes(StandardCharsets.UTF_8));
    metainfo.put("info", info);
    Manifest manifest = MetadataFetcher.parseTorrent(Bencode.encode(metainfo));
    assertEquals("authorized-release.iso", manifest.name());
    assertEquals(128L, manifest.totalSize());
    assertEquals(40, manifest.infoHash().length());
  }

  private static String hex(byte[] bytes) {
    var output = new StringBuilder();
    for (byte value : bytes) output.append(String.format(Locale.ROOT, "%02x", value & 0xff));
    return output.toString();
  }
}
