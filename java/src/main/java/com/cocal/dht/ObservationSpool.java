package com.cocal.dht;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Small SQLite-backed overflow buffer used only while Redis is unavailable or backlogged. */
final class ObservationSpool implements AutoCloseable {
  private static final int MAX_ROWS = 1_000_000;
  private final Connection connection;

  ObservationSpool(Path path) throws Exception {
    connection = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath());
    try (var statement = connection.createStatement()) {
      statement.executeUpdate("PRAGMA journal_mode=WAL");
      statement.executeUpdate("PRAGMA synchronous=NORMAL");
      statement.executeUpdate("CREATE TABLE IF NOT EXISTS observation_spool(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,info_hash TEXT NOT NULL,observed_at TEXT NOT NULL,query TEXT NOT NULL,peer_host TEXT,peer_port INTEGER,source_host TEXT,source_port INTEGER)");
      statement.executeUpdate("CREATE INDEX IF NOT EXISTS observation_spool_seq_idx ON observation_spool(seq)");
    }
  }

  synchronized void append(List<DhtObservation> observations) throws Exception {
    if (observations == null || observations.isEmpty()) return;
    connection.setAutoCommit(false);
    try (PreparedStatement statement = connection.prepareStatement(
        "INSERT OR IGNORE INTO observation_spool(event_id,info_hash,observed_at,query,peer_host,peer_port,source_host,source_port) VALUES(?,?,?,?,?,?,?,?)")) {
      for (DhtObservation observation : observations) {
        statement.setString(1, observation.eventId());
        statement.setString(2, observation.infoHash());
        statement.setString(3, observation.observedAt().toString());
        statement.setString(4, observation.query());
        bindEndpoint(statement, 5, 6, observation.peer());
        bindEndpoint(statement, 7, 8, observation.source());
        statement.addBatch();
      }
      statement.executeBatch();
      try (var count = connection.createStatement().executeQuery("SELECT count(*) FROM observation_spool")) {
        if (count.next() && count.getLong(1) > MAX_ROWS) {
          try (var trim = connection.prepareStatement("DELETE FROM observation_spool WHERE seq IN (SELECT seq FROM observation_spool ORDER BY seq LIMIT ?" +
              ")")) {
            trim.setLong(1, count.getLong(1) - MAX_ROWS);
            trim.executeUpdate();
          }
        }
      }
      connection.commit();
    } catch (Exception error) {
      connection.rollback();
      throw error;
    } finally { connection.setAutoCommit(true); }
  }

  synchronized List<DhtObservation> read(int limit) throws Exception {
    List<DhtObservation> result = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(
        "SELECT event_id,info_hash,observed_at,query,peer_host,peer_port,source_host,source_port FROM observation_spool ORDER BY seq LIMIT ?")) {
      statement.setInt(1, limit);
      try (ResultSet rows = statement.executeQuery()) {
        while (rows.next()) {
          result.add(new DhtObservation(rows.getString(1), rows.getString(2), Instant.parse(rows.getString(3)),
              rows.getString(4), endpoint(rows.getString(5), rows.getObject(6, Integer.class)),
              endpoint(rows.getString(7), rows.getObject(8, Integer.class))));
        }
      }
    }
    return result;
  }

  synchronized void remove(List<DhtObservation> observations) throws Exception {
    if (observations == null || observations.isEmpty()) return;
    try (PreparedStatement statement = connection.prepareStatement("DELETE FROM observation_spool WHERE event_id=?")) {
      for (DhtObservation observation : observations) { statement.setString(1, observation.eventId()); statement.addBatch(); }
      statement.executeBatch();
    }
  }

  synchronized long size() throws Exception {
    try (var rows = connection.createStatement().executeQuery("SELECT count(*) FROM observation_spool")) { return rows.next() ? rows.getLong(1) : 0; }
  }

  private static void bindEndpoint(PreparedStatement statement, int hostIndex, int portIndex, InetSocketAddress endpoint) throws Exception {
    if (endpoint == null) { statement.setNull(hostIndex, java.sql.Types.VARCHAR); statement.setNull(portIndex, java.sql.Types.INTEGER); }
    else { statement.setString(hostIndex, endpoint.getHostString()); statement.setInt(portIndex, endpoint.getPort()); }
  }

  private static InetSocketAddress endpoint(String host, Integer port) {
    return host == null || port == null ? null : new InetSocketAddress(host, port);
  }

  @Override public synchronized void close() {
    try { connection.close(); } catch (Exception ignored) { }
  }
}
