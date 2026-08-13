package com.cocal.dht;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.net.URI;

final class Catalog implements AutoCloseable {
  private static final int TOUCH_BATCH_SIZE = 1_000;
  private final HikariDataSource dataSource;
  Catalog(String url, String user, String password, int poolSize) {
    URI parsed = parseUri(url);
    HikariConfig config = new HikariConfig(); config.setJdbcUrl(jdbcUrl(url, parsed));
    if (user != null && !user.isBlank()) config.setUsername(user); else if (parsed != null && parsed.getUserInfo() != null) config.setUsername(decodeUserInfo(parsed.getUserInfo())[0]);
    if (password != null && !password.isBlank()) config.setPassword(password); else if (parsed != null && parsed.getUserInfo() != null) config.setPassword(decodeUserInfo(parsed.getUserInfo())[1]);
    config.setMaximumPoolSize(poolSize); config.setMinimumIdle(Math.min(1, poolSize)); config.setPoolName("dht-collector");
    dataSource = new HikariDataSource(config);
  }
  private static URI parseUri(String url) { try { return url.startsWith("jdbc:") ? null : URI.create(url); } catch (IllegalArgumentException error) { throw new IllegalArgumentException("invalid PostgreSQL URL", error); } }
  private static String[] decodeUserInfo(String userInfo) { int split=userInfo.indexOf(':'); String user=split<0?userInfo:userInfo.substring(0,split); String password=split<0?"":userInfo.substring(split+1); return new String[]{java.net.URLDecoder.decode(user,java.nio.charset.StandardCharsets.UTF_8),java.net.URLDecoder.decode(password,java.nio.charset.StandardCharsets.UTF_8)}; }
  private static String jdbcUrl(String original, URI uri) { if (original.startsWith("jdbc:")) return original; return "jdbc:postgresql://" + uri.getRawAuthority().replaceFirst("^[^@]*@", "") + uri.getRawPath() + (uri.getRawQuery()==null?"":"?"+uri.getRawQuery()); }
  void initialize() throws SQLException {
    try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
      s.executeUpdate("CREATE TABLE IF NOT EXISTS discovered_resource (info_hash text PRIMARY KEY, first_seen_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL, source text NOT NULL, state text NOT NULL DEFAULT 'active')");
      s.executeUpdate("CREATE INDEX IF NOT EXISTS discovered_resource_state_seen_idx ON discovered_resource (state, last_seen_at)");
      s.executeUpdate("CREATE TABLE IF NOT EXISTS probe_event (event_id text PRIMARY KEY, event_type text NOT NULL, occurred_at timestamptz NOT NULL, info_hash text, peer_host text, peer_port integer, source_host text, source_port integer, mode text, message text, raw_event jsonb NOT NULL)");
    }
  }
  Map<String, Instant> recentResources(Instant cutoff) throws SQLException {
    Map<String, Instant> result = new LinkedHashMap<>();
    try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement("SELECT info_hash,last_seen_at FROM discovered_resource WHERE state='active' AND last_seen_at >= ?")) {
      p.setTimestamp(1, Timestamp.from(cutoff)); try (ResultSet rs = p.executeQuery()) { while (rs.next()) result.put(rs.getString(1), rs.getTimestamp(2).toInstant()); }
    } return result;
  }
  long countDiscoveredResources() throws SQLException {
    try (Connection c=dataSource.getConnection(); Statement s=c.createStatement(); ResultSet r=s.executeQuery("SELECT count(*) FROM discovered_resource")) { return r.next() ? r.getLong(1) : 0; }
  }
  boolean exists(String hash) throws SQLException { try (Connection c=dataSource.getConnection(); PreparedStatement p=c.prepareStatement("SELECT 1 FROM discovered_resource WHERE info_hash=?")) { p.setString(1,hash); try(ResultSet r=p.executeQuery()){return r.next();} } }
  boolean claim(String hash, Instant at, String source) throws SQLException {
    try (Connection c=dataSource.getConnection(); PreparedStatement p=c.prepareStatement("INSERT INTO discovered_resource(info_hash,first_seen_at,last_seen_at,source,state) VALUES(?,?,?,?,'active') ON CONFLICT(info_hash) DO UPDATE SET last_seen_at=greatest(discovered_resource.last_seen_at,excluded.last_seen_at),state='active' RETURNING (xmax=0)")) {
      p.setString(1,hash); p.setTimestamp(2,Timestamp.from(at)); p.setTimestamp(3,Timestamp.from(at)); p.setString(4,source); try(ResultSet r=p.executeQuery()){return r.next() && r.getBoolean(1);} }
  }
  void touch(Map<String, Instant> observations) throws SQLException {
    if (observations.isEmpty()) return;
    try (Connection connection = dataSource.getConnection();
         PreparedStatement statement = connection.prepareStatement(
             "UPDATE discovered_resource SET last_seen_at=greatest(last_seen_at,?),state='active' WHERE info_hash=?")) {
      connection.setAutoCommit(false);
      int pending = 0;
      try {
        for (var observation : observations.entrySet()) {
          statement.setTimestamp(1, Timestamp.from(observation.getValue()));
          statement.setString(2, observation.getKey());
          statement.addBatch();
          if (++pending == TOUCH_BATCH_SIZE) {
            statement.executeBatch();
            statement.clearBatch();
            pending = 0;
          }
        }
        if (pending > 0) statement.executeBatch();
        connection.commit();
      } catch (SQLException error) {
        connection.rollback();
        throw error;
      } finally {
        connection.setAutoCommit(true);
      }
    }
  }
  void event(String type, String hash, String rawJson) throws SQLException {
    try(Connection c=dataSource.getConnection(); PreparedStatement p=c.prepareStatement("INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,mode,raw_event) VALUES(?,?,?,?,'passive',?::jsonb) ON CONFLICT DO NOTHING")){p.setString(1,UUID.randomUUID().toString());p.setString(2,type);p.setTimestamp(3,Timestamp.from(Instant.now()));p.setString(4,hash);p.setString(5,rawJson);p.executeUpdate();}
  }
  @Override public void close(){dataSource.close();}
}
