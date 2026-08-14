package com.cocal.dht;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class DirectMetadataFetcherTest {
  private static final int PIECE_SIZE = 16 * 1024;

  @Test
  void oneFetchTimeoutDoesNotCloseAnotherFetchSockets() throws Exception {
    byte[] metadata = Bencode.encode(Map.of("length", 123L, "name", "fixture.bin"));
    String infoHash = hex(MessageDigest.getInstance("SHA-1").digest(metadata));
    try (ServerSocket stalledServer = server(); ServerSocket metadataServer = server();
         var servers = Executors.newVirtualThreadPerTaskExecutor();
         var fetcher = new DirectMetadataFetcher()) {
      servers.submit(() -> stall(stalledServer));
      servers.submit(() -> serveMetadata(metadataServer, metadata, 1_200));
      var stalled = fetcher.fetch(infoHash, List.of(endpoint(stalledServer)), 1).toCompletableFuture();
      var successful = fetcher.fetch(infoHash, List.of(endpoint(metadataServer)), 4).toCompletableFuture();

      assertTrue(stalled.get(3, TimeUnit.SECONDS).isEmpty());
      Optional<byte[]> result = successful.get(6, TimeUnit.SECONDS);
      assertTrue(result.isPresent());
      assertArrayEquals(metadata, result.orElseThrow());
    }
  }

  @Test
  void retriesATemporarilyRejectedPiece() throws Exception {
    byte[] metadata = Bencode.encode(Map.of("length", 456L, "name", "retry.bin"));
    String infoHash = hex(MessageDigest.getInstance("SHA-1").digest(metadata));
    try (ServerSocket metadataServer = server();
         var servers = Executors.newVirtualThreadPerTaskExecutor();
         var fetcher = new DirectMetadataFetcher()) {
      servers.submit(() -> serveMetadataAfterReject(metadataServer, metadata));

      Optional<byte[]> result = fetcher.fetch(infoHash, List.of(endpoint(metadataServer)), 3)
          .toCompletableFuture().get(5, TimeUnit.SECONDS);

      assertTrue(result.isPresent());
      assertArrayEquals(metadata, result.orElseThrow());
    }
  }

  @Test
  void advancesToAnotherPeerBatch() throws Exception {
    byte[] metadata = Bencode.encode(Map.of("length", 789L, "name", "later-batch.bin"));
    String infoHash = hex(MessageDigest.getInstance("SHA-1").digest(metadata));
    List<InetSocketAddress> peers = new ArrayList<>();
    for (int index = 0; index < 4; index++) {
      try (ServerSocket unavailable = server()) {
        peers.add(endpoint(unavailable));
      }
    }
    try (ServerSocket metadataServer = server();
         var servers = Executors.newVirtualThreadPerTaskExecutor();
         var fetcher = new DirectMetadataFetcher()) {
      peers.add(endpoint(metadataServer));
      servers.submit(() -> serveMetadata(metadataServer, metadata, 0));

      Optional<byte[]> result = fetcher.fetch(infoHash, peers, 4)
          .toCompletableFuture().get(6, TimeUnit.SECONDS);

      assertTrue(result.isPresent());
      assertArrayEquals(metadata, result.orElseThrow());
    }
  }

  private static ServerSocket server() throws Exception {
    return new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
  }

  private static InetSocketAddress endpoint(ServerSocket server) {
    return new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort());
  }

  private static void stall(ServerSocket server) {
    try (Socket socket = server.accept()) {
      socket.getInputStream().readNBytes(68);
      Thread.sleep(2_000);
    } catch (Exception ignored) { }
  }

  private static void serveMetadata(ServerSocket server, byte[] metadata, long delayMillis) {
    try (Socket socket = server.accept()) {
      DataInputStream input = new DataInputStream(socket.getInputStream());
      DataOutputStream output = new DataOutputStream(socket.getOutputStream());
      byte[] handshake = input.readNBytes(68);
      Thread.sleep(delayMillis);
      output.write(handshake);
      output.flush();
      readFrame(input); // Client extension handshake.
      writeFrame(output, extended(0, Bencode.encode(Map.of(
          "m", Map.of("ut_metadata", 2L), "metadata_size", (long) metadata.length))));
      writeFrame(output, new byte[]{1}); // A legal one-byte unchoke message.

      int pieces = (metadata.length + PIECE_SIZE - 1) / PIECE_SIZE;
      for (int ignored = 0; ignored < pieces; ignored++) {
        byte[] request = readFrame(input);
        Bencode.Decoded decoded = Bencode.decodePrefix(Arrays.copyOfRange(request, 2, request.length));
        int piece = Math.toIntExact(((Number) Bencode.dict(decoded.value()).get("piece")).longValue());
        int start = piece * PIECE_SIZE;
        int end = Math.min(metadata.length, start + PIECE_SIZE);
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("msg_type", 1L);
        header.put("piece", (long) piece);
        header.put("total_size", (long) metadata.length);
        byte[] encoded = Bencode.encode(header);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(20);
        payload.write(1); // ID the client advertised for incoming ut_metadata messages.
        payload.writeBytes(encoded);
        payload.write(metadata, start, end - start);
        writeFrame(output, payload.toByteArray());
      }
    } catch (Exception error) {
      throw new RuntimeException(error);
    }
  }

  private static void serveMetadataAfterReject(ServerSocket server, byte[] metadata) {
    try (Socket socket = server.accept()) {
      DataInputStream input = new DataInputStream(socket.getInputStream());
      DataOutputStream output = new DataOutputStream(socket.getOutputStream());
      byte[] handshake = input.readNBytes(68);
      output.write(handshake);
      output.flush();
      readFrame(input);
      writeFrame(output, extended(0, Bencode.encode(Map.of(
          "m", Map.of("ut_metadata", 2L), "metadata_size", (long) metadata.length))));

      byte[] firstRequest = readFrame(input);
      Bencode.Decoded decoded = Bencode.decodePrefix(Arrays.copyOfRange(
          firstRequest, 2, firstRequest.length));
      long piece = ((Number) Bencode.dict(decoded.value()).get("piece")).longValue();
      writeFrame(output, extended(1, Bencode.encode(Map.of("msg_type", 2L, "piece", piece))));
      readFrame(input); // Retried request.

      Map<String, Object> header = new LinkedHashMap<>();
      header.put("msg_type", 1L);
      header.put("piece", piece);
      header.put("total_size", (long) metadata.length);
      ByteArrayOutputStream payload = new ByteArrayOutputStream();
      payload.write(20);
      payload.write(1);
      payload.writeBytes(Bencode.encode(header));
      payload.writeBytes(metadata);
      writeFrame(output, payload.toByteArray());
    } catch (Exception error) {
      throw new RuntimeException(error);
    }
  }

  private static byte[] extended(int extensionId, byte[] body) {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    payload.write(20);
    payload.write(extensionId);
    payload.writeBytes(body);
    return payload.toByteArray();
  }

  private static byte[] readFrame(DataInputStream input) throws Exception {
    int length = input.readInt();
    return input.readNBytes(length);
  }

  private static void writeFrame(DataOutputStream output, byte[] payload) throws Exception {
    output.writeInt(payload.length);
    output.write(payload);
    output.flush();
  }

  private static String hex(byte[] bytes) {
    StringBuilder value = new StringBuilder(bytes.length * 2);
    for (byte item : bytes) value.append(String.format("%02x", item & 0xff));
    return value.toString();
  }
}
