package com.cocal.dht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Fetches BEP-9 metadata directly from peers learned from announce_peer. */
final class DirectMetadataFetcher implements AutoCloseable {
  private static final int HANDSHAKE_LENGTH = 68;
  private static final int EXTENDED_MESSAGE_ID = 20;
  private static final int EXTENDED_HANDSHAKE_ID = 0;
  private static final int PIECE_SIZE = 16 * 1024;
  private static final int MAX_METADATA_BYTES = MetadataFetcher.MAX_METADATA_BYTES;
  private static final int MAX_FRAME_BYTES = MAX_METADATA_BYTES + 64 * 1024;
  static final int MAX_PEERS_PER_FETCH = 12;
  private static final int PEER_BATCH_SIZE = 4;
  private static final byte[] PROTOCOL = "BitTorrent protocol".getBytes(StandardCharsets.ISO_8859_1);

  private final ExecutorService workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
  private final SecureRandom random = new SecureRandom();
  private final Set<Socket> activeSockets = java.util.concurrent.ConcurrentHashMap.newKeySet();

  CompletionStage<Optional<byte[]>> fetch(String infoHash, Collection<InetSocketAddress> peers,
                                          int timeoutSeconds) {
    List<InetSocketAddress> candidates = peers == null ? List.of() : peers.stream()
        .filter(peer -> peer != null && peer.getAddress() != null && peer.getPort() > 0 && peer.getPort() <= 65535)
        .distinct().limit(MAX_PEERS_PER_FETCH).toList();
    if (candidates.isEmpty()) return CompletableFuture.completedFuture(Optional.empty());
    long timeoutMillis = Math.max(1_000L, timeoutSeconds * 1_000L);
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    CompletableFuture<Optional<byte[]>> result = new CompletableFuture<>();
    List<Future<?>> tasks = new CopyOnWriteArrayList<>();
    Set<Socket> fetchSockets = java.util.concurrent.ConcurrentHashMap.newKeySet();
    launchBatch(infoHash, candidates, 0, deadline, result, tasks, fetchSockets);
    CompletableFuture.delayedExecutor(timeoutMillis, TimeUnit.MILLISECONDS).execute(() -> {
      if (result.complete(Optional.empty())) {
        closeSockets(fetchSockets);
        tasks.forEach(task -> task.cancel(true));
      }
    });
    return result;
  }

  private void launchBatch(String infoHash, List<InetSocketAddress> candidates, int offset,
                           long deadline, CompletableFuture<Optional<byte[]>> result,
                           List<Future<?>> tasks, Set<Socket> fetchSockets) {
    if (result.isDone()) return;
    if (offset >= candidates.size() || System.nanoTime() >= deadline) {
      if (result.complete(Optional.empty())) closeSockets(fetchSockets);
      return;
    }
    int end = Math.min(candidates.size(), offset + PEER_BATCH_SIZE);
    AtomicInteger remaining = new AtomicInteger(end - offset);
    for (int index = offset; index < end; index++) {
      InetSocketAddress peer = candidates.get(index);
      tasks.add(workers.submit(() -> {
        try {
          byte[] metadata = fetchPeer(infoHash, peer, deadline, fetchSockets);
          if (metadata != null && result.complete(Optional.of(metadata))) closeSockets(fetchSockets);
        } catch (Exception ignored) {
          // A DHT peer is untrusted and often does not expose BEP-9. Try the next peer.
        } finally {
          if (remaining.decrementAndGet() == 0 && !result.isDone()) {
            launchBatch(infoHash, candidates, end, deadline, result, tasks, fetchSockets);
          }
        }
      }));
    }
  }

  private byte[] fetchPeer(String infoHash, InetSocketAddress peer, long deadline,
                           Set<Socket> fetchSockets) throws Exception {
    Socket socket = new Socket();
    try (socket) {
      activeSockets.add(socket);
      fetchSockets.add(socket);
      socket.setTcpNoDelay(true);
      socket.setKeepAlive(false);
      long remainingMillis = remainingMillis(deadline);
      socket.connect(peer, timeoutMillisToInt(Math.min(remainingMillis, 3_000L)));
      socket.setSoTimeout(timeoutMillisToInt(Math.min(remainingMillis, 5_000L)));
      DataInputStream input = new DataInputStream(socket.getInputStream());
      DataOutputStream output = new DataOutputStream(socket.getOutputStream());
      byte[] expectedHash = hex(infoHash);
      output.writeByte(PROTOCOL.length);
      output.write(PROTOCOL);
      output.write(new byte[5]);
      output.writeByte(0x10); // BEP-10 extension protocol bit.
      output.write(new byte[2]);
      output.write(expectedHash);
      output.write(peerId());
      output.flush();

      byte[] handshake = readFully(input, socket, HANDSHAKE_LENGTH, deadline);
      validateHandshake(handshake, expectedHash);
      sendExtendedHandshake(output);

      int metadataExtension = 1; // ID advertised in our extended handshake.
      int requestExtension = -1;  // ID advertised by the remote peer.
      int totalSize = -1;
      int pieceCount = -1;
      byte[][] pieces = null;
      int nextPiece = 0;
      int receivedPieces = 0;
      int metadataRetries = 0;
      boolean pipelineInitialized = false;
      Map<Integer, Integer> rejectedPieces = new java.util.HashMap<>();
      while (System.nanoTime() < deadline) {
        int frameLength = readInt(input, socket, deadline);
        if (frameLength == 0) continue;
        if (frameLength < 1 || frameLength > MAX_FRAME_BYTES) {
          throw new IOException("invalid peer frame length: " + frameLength);
        }
        byte[] frame = readFully(input, socket, frameLength, deadline);
        if ((frame[0] & 0xff) != EXTENDED_MESSAGE_ID) continue;
        int extensionId = frame[1] & 0xff;
        if (extensionId == EXTENDED_HANDSHAKE_ID) {
          Map<String, Object> handshakeInfo = parseDictionary(frame, 2);
          Map<String, Object> extensions = Bencode.dict(handshakeInfo.get("m"));
          long id = number(extensions.get("ut_metadata"));
          if (id <= 0 || id > 255) throw new IOException("peer does not support ut_metadata");
          requestExtension = (int) id;
          long advertisedSize = number(handshakeInfo.get("metadata_size"));
          if (advertisedSize > 0) {
            if (advertisedSize > MAX_METADATA_BYTES) throw new IOException("invalid metadata size");
            totalSize = Math.toIntExact(advertisedSize);
            pieceCount = (totalSize + PIECE_SIZE - 1) / PIECE_SIZE;
            pieces = new byte[pieceCount][];
            for (int piece = 0; piece < pieceCount; piece++) {
              sendMetadataRequest(output, requestExtension, piece);
            }
            pipelineInitialized = true;
          } else {
            sendMetadataRequest(output, requestExtension, 0);
            nextPiece = 1;
          }
          continue;
        }
        if (requestExtension < 0 || extensionId != metadataExtension) continue;
        Bencode.Decoded decoded = Bencode.decodePrefix(Arrays.copyOfRange(frame, 2, frame.length));
        Map<String, Object> message = Bencode.dict(decoded.value());
        int messageType = Math.toIntExact(number(message.get("msg_type")));
        int piece = Math.toIntExact(number(message.get("piece")));
        if (messageType == 2) {
          if (piece < 0 || rejectedPieces.merge(piece, 1, Integer::sum) > 2) {
            throw new IOException("peer repeatedly rejected metadata request");
          }
          sendMetadataRequest(output, requestExtension, piece);
          continue;
        }
        if (messageType != 1) continue;
        if (piece < 0) throw new IOException("invalid metadata piece");
        if (totalSize < 0) {
          totalSize = Math.toIntExact(number(message.get("total_size")));
          if (totalSize < 1 || totalSize > MAX_METADATA_BYTES) throw new IOException("invalid metadata size");
          pieceCount = (totalSize + PIECE_SIZE - 1) / PIECE_SIZE;
          pieces = new byte[pieceCount][];
        }
        if (piece >= pieceCount || pieces[piece] != null) continue;
        int dataStart = 2 + decoded.consumed();
        byte[] data = Arrays.copyOfRange(frame, dataStart, frame.length);
        int expectedLength = Math.min(PIECE_SIZE, totalSize - piece * PIECE_SIZE);
        if (data.length != expectedLength) throw new IOException("invalid metadata piece length");
        pieces[piece] = data;
        receivedPieces++;
        if (receivedPieces == pieceCount) {
          byte[] metadata = join(pieces, totalSize);
          if (!MessageDigest.isEqual(digest("SHA-1", metadata), expectedHash)) {
            if (metadataRetries++ >= 1) throw new IOException("metadata hash mismatch");
            Arrays.fill(pieces, null);
            receivedPieces = 0;
            rejectedPieces.clear();
            for (int retryPiece = 0; retryPiece < pieceCount; retryPiece++) {
              sendMetadataRequest(output, requestExtension, retryPiece);
            }
            continue;
          }
          return metadata;
        }
        if (!pipelineInitialized) {
          pipelineInitialized = true;
          while (nextPiece < pieceCount) sendMetadataRequest(output, requestExtension, nextPiece++);
        } else if (nextPiece < pieceCount) {
          sendMetadataRequest(output, requestExtension, nextPiece++);
        }
      }
      throw new IOException("metadata peer timeout");
    } finally {
      fetchSockets.remove(socket);
      activeSockets.remove(socket);
    }
  }

  private static void validateHandshake(byte[] handshake, byte[] expectedHash) throws IOException {
    if (handshake[0] != PROTOCOL.length || !Arrays.equals(PROTOCOL, Arrays.copyOfRange(handshake, 1, 20))) {
      throw new IOException("invalid BitTorrent handshake");
    }
    if ((handshake[25] & 0x10) == 0) throw new IOException("peer lacks extension protocol");
    if (!MessageDigest.isEqual(expectedHash, Arrays.copyOfRange(handshake, 28, 48))) {
      throw new IOException("peer returned unexpected infohash");
    }
  }

  private static void sendExtendedHandshake(DataOutputStream output) throws IOException {
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("ut_metadata", 1L);
    Map<String, Object> extensions = new LinkedHashMap<>();
    extensions.put("ut_metadata", metadata.get("ut_metadata"));
    Map<String, Object> handshake = new LinkedHashMap<>();
    handshake.put("m", extensions);
    handshake.put("v", "dht-collector-java");
    byte[] encoded = Bencode.encode(handshake);
    output.writeInt(2 + encoded.length);
    output.writeByte(EXTENDED_MESSAGE_ID);
    output.writeByte(EXTENDED_HANDSHAKE_ID);
    output.write(encoded);
    output.flush();
  }

  private static void sendMetadataRequest(DataOutputStream output, int extensionId, int piece) throws IOException {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("msg_type", 0L);
    request.put("piece", (long) piece);
    byte[] encoded = Bencode.encode(request);
    output.writeInt(2 + encoded.length);
    output.writeByte(EXTENDED_MESSAGE_ID);
    output.writeByte(extensionId);
    output.write(encoded);
    output.flush();
  }

  private static Map<String, Object> parseDictionary(byte[] frame, int start) {
    byte[] encoded = Arrays.copyOfRange(frame, start, frame.length);
    return Bencode.dict(Bencode.decodePrefix(encoded).value());
  }

  private static byte[] join(byte[][] pieces, int length) {
    byte[] result = new byte[length];
    int offset = 0;
    for (byte[] piece : pieces) {
      System.arraycopy(piece, 0, result, offset, piece.length);
      offset += piece.length;
    }
    return result;
  }

  private static int readInt(DataInputStream input, Socket socket, long deadline) throws IOException {
    byte[] bytes = readFully(input, socket, 4, deadline);
    return ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
  }

  private static byte[] readFully(DataInputStream input, Socket socket, int length, long deadline) throws IOException {
    byte[] result = new byte[length];
    int offset = 0;
    while (offset < length) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) throw new IOException("metadata peer timeout");
      socket.setSoTimeout(timeoutMillisToInt(Math.min(TimeUnit.NANOSECONDS.toMillis(remaining), 5_000L)));
      int count;
      try { count = input.read(result, offset, length - offset); }
      catch (java.net.SocketTimeoutException error) { throw new IOException("metadata peer timeout", error); }
      if (count < 0) throw new EOFException("peer closed connection");
      offset += count;
    }
    return result;
  }

  private byte[] peerId() {
    byte[] peerId = new byte[20];
    byte[] prefix = "-CD0001-".getBytes(StandardCharsets.US_ASCII);
    System.arraycopy(prefix, 0, peerId, 0, prefix.length);
    byte[] randomPart = new byte[peerId.length - prefix.length];
    random.nextBytes(randomPart);
    System.arraycopy(randomPart, 0, peerId, prefix.length, randomPart.length);
    return peerId;
  }

  private static void closeSockets(Set<Socket> sockets) {
    for (Socket socket : sockets) {
      try { socket.close(); } catch (IOException ignored) { }
    }
  }

  private static long number(Object value) {
    return value instanceof Number number ? number.longValue() : -1;
  }

  private static byte[] hex(String value) {
    int length = value.length();
    if ((length & 1) != 0) throw new IllegalArgumentException("invalid info-hash");
    byte[] result = new byte[length / 2];
    for (int index = 0; index < result.length; index++) {
      int high = Character.digit(value.charAt(index * 2), 16);
      int low = Character.digit(value.charAt(index * 2 + 1), 16);
      if (high < 0 || low < 0) throw new IllegalArgumentException("invalid info-hash");
      result[index] = (byte) ((high << 4) | low);
    }
    return result;
  }

  private static byte[] digest(String algorithm, byte[] bytes) {
    try { return MessageDigest.getInstance(algorithm).digest(bytes); }
    catch (Exception error) { throw new IllegalStateException(error); }
  }

  private static int timeoutMillisToInt(long millis) {
    return (int) Math.max(1L, Math.min(Integer.MAX_VALUE, millis));
  }

  private static long remainingMillis(long deadline) throws IOException {
    long remaining = deadline - System.nanoTime();
    if (remaining <= 0) throw new IOException("metadata peer timeout");
    return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining));
  }

  @Override public void close() {
    closeSockets(activeSockets);
    workers.close();
  }
}
