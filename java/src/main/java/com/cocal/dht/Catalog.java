package com.cocal.dht;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

final class Catalog implements AutoCloseable {
  private static final int TOUCH_BATCH_SIZE = 1_000;
  private static final String CONTENT_SEARCH_HEAD = "to_tsvector('simple', left(coalesce(c.name,'') || ' ' || coalesce(c.files_text,''), 800000))";
  private static final String CONTENT_SEARCH_TAIL = "to_tsvector('simple', substring(coalesce(c.files_text,'') from 700001 for 800000))";
  private static final String CONTENT_SEARCH_MATCH = "(" + CONTENT_SEARCH_HEAD + " @@ s.term OR (length(c.files_text) > 700000 AND " + CONTENT_SEARCH_TAIL + " @@ s.term))";
  private final HikariDataSource dataSource;
  private final Map<String, AtomicLong> pendingCounters = new ConcurrentHashMap<>();

  record ManifestWrite(boolean inserted, boolean changed) {}
  record ObservationWrite(Set<String> freshHashes, Set<String> immediateHashes) {}

  Catalog(String url, String user, String password, int poolSize) {
    URI parsed = parseUri(url);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl(url, parsed));
    String[] embedded = parsed == null || parsed.getUserInfo() == null ? null : decodeUserInfo(parsed.getUserInfo());
    if (user != null && !user.isBlank()) config.setUsername(user);
    else if (embedded != null) config.setUsername(embedded[0]);
    if (password != null && !password.isBlank()) config.setPassword(password);
    else if (embedded != null) config.setPassword(embedded[1]);
    config.setMaximumPoolSize(poolSize);
    config.setMinimumIdle(Math.min(1, poolSize));
    config.setPoolName("dht-collector");
    config.setConnectionTimeout(10_000);
    config.setValidationTimeout(3_000);
    config.setKeepaliveTime(Duration.ofMinutes(1).toMillis());
    config.setMaxLifetime(Duration.ofMinutes(10).toMillis());
    config.addDataSourceProperty("tcpKeepAlive", "true");
    dataSource = new HikariDataSource(config);
  }

  private static URI parseUri(String url) {
    try { return url.startsWith("jdbc:") ? null : URI.create(url); }
    catch (IllegalArgumentException error) { throw new IllegalArgumentException("invalid PostgreSQL URL", error); }
  }

  private static String[] decodeUserInfo(String userInfo) {
    int split = userInfo.indexOf(':');
    String user = split < 0 ? userInfo : userInfo.substring(0, split);
    String password = split < 0 ? "" : userInfo.substring(split + 1);
    return new String[]{URLDecoder.decode(user, StandardCharsets.UTF_8), URLDecoder.decode(password, StandardCharsets.UTF_8)};
  }

  private static String jdbcUrl(String original, URI uri) {
    if (original.startsWith("jdbc:")) return original;
    return "jdbc:postgresql://" + uri.getRawAuthority().replaceFirst("^[^@]*@", "") + uri.getRawPath()
        + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
  }

  void initialize() throws SQLException {
    try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
      try (ResultSet ignored = statement.executeQuery("SELECT pg_advisory_lock(hashtextextended('dht-collector-schema', 0))")) { }
      try {
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS content (content_id text PRIMARY KEY, info_hash text NOT NULL UNIQUE, variant text NOT NULL, name text NOT NULL, total_size bigint NOT NULL, file_count integer NOT NULL, metadata_size integer NOT NULL, metadata_sha256 text NOT NULL, policy_state text NOT NULL DEFAULT 'approved', files_text text NOT NULL DEFAULT '', created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)");
        statement.executeUpdate("ALTER TABLE content ADD COLUMN IF NOT EXISTS files_text text NOT NULL DEFAULT ''");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS content_updated_idx ON content (updated_at DESC)");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS file_entry (content_id text NOT NULL REFERENCES content(content_id) ON DELETE CASCADE, ordinal integer NOT NULL, path text NOT NULL, size bigint NOT NULL, PRIMARY KEY (content_id, ordinal))");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS discovered_resource (info_hash text PRIMARY KEY, first_seen_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL, source text NOT NULL, state text NOT NULL DEFAULT 'active')");
        statement.executeUpdate("ALTER TABLE discovered_resource ADD COLUMN IF NOT EXISTS last_seen_at timestamptz");
        statement.executeUpdate("ALTER TABLE discovered_resource ADD COLUMN IF NOT EXISTS state text NOT NULL DEFAULT 'active'");
        statement.executeUpdate("UPDATE discovered_resource SET last_seen_at=COALESCE(last_seen_at, first_seen_at),state=COALESCE(state,'active') WHERE last_seen_at IS NULL OR state IS NULL");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS discovered_resource_state_seen_idx ON discovered_resource (state, last_seen_at)");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS probe_event (event_id text PRIMARY KEY, event_type text NOT NULL, occurred_at timestamptz NOT NULL, info_hash text, peer_host text, peer_port integer, source_host text, source_port integer, mode text, message text, raw_event jsonb NOT NULL)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS probe_event_occurred_at_idx ON probe_event (occurred_at DESC)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS probe_event_info_hash_idx ON probe_event (info_hash, occurred_at DESC)");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS metadata_job (info_hash text PRIMARY KEY, priority integer NOT NULL DEFAULT 0, attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS metadata_job_due_idx ON metadata_job (priority DESC, next_attempt_at ASC, updated_at DESC)");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS catalog_counter (name text PRIMARY KEY, value bigint NOT NULL DEFAULT 0)");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS minute_metric (bucket timestamptz PRIMARY KEY, links bigint NOT NULL DEFAULT 0, queries bigint NOT NULL DEFAULT 0, failures bigint NOT NULL DEFAULT 0, warnings bigint NOT NULL DEFAULT 0, indexed bigint NOT NULL DEFAULT 0)");
        statement.executeUpdate("ALTER TABLE minute_metric ADD COLUMN IF NOT EXISTS indexed bigint NOT NULL DEFAULT 0");
      } finally {
        try (ResultSet ignored = statement.executeQuery("SELECT pg_advisory_unlock(hashtextextended('dht-collector-schema', 0))")) { }
      }
    }
  }

  int loadRecentResources(Instant cutoff, BiConsumer<String, Instant> consumer) throws SQLException {
    int count = 0;
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement("SELECT info_hash,last_seen_at FROM discovered_resource WHERE state='active' AND last_seen_at >= ?")) {
        statement.setFetchSize(10_000);
        statement.setTimestamp(1, Timestamp.from(cutoff));
        try (ResultSet rows = statement.executeQuery()) {
          while (rows.next()) {
            consumer.accept(rows.getString(1), rows.getTimestamp(2).toInstant());
            count++;
          }
        }
      }
      connection.commit();
    }
    return count;
  }

  long countDiscoveredResources() throws SQLException {
    try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT count(*) FROM discovered_resource")) { return rows.next() ? rows.getLong(1) : 0; }
  }

  boolean exists(String hash) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM discovered_resource WHERE info_hash=?")) { statement.setString(1, hash); try (ResultSet rows = statement.executeQuery()) { return rows.next(); } }
  }

  boolean claim(String hash, Instant at, String source) throws SQLException {
    boolean fresh;
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("WITH upsert AS (INSERT INTO discovered_resource(info_hash,first_seen_at,last_seen_at,source,state) VALUES(?,?,?,?,'active') ON CONFLICT(info_hash) DO UPDATE SET last_seen_at=greatest(discovered_resource.last_seen_at,excluded.last_seen_at),state='active' RETURNING (xmax=0) AS fresh) SELECT fresh FROM upsert")) {
      statement.setString(1, hash); statement.setTimestamp(2, Timestamp.from(at)); statement.setTimestamp(3, Timestamp.from(at)); statement.setString(4, source);
      try (ResultSet rows = statement.executeQuery()) { fresh = rows.next() && rows.getBoolean(1); }
    }
    if (fresh) queueCounter("discovered", 1);
    return fresh;
  }

  void touch(Map<String, Instant> observations) throws SQLException {
    if (observations.isEmpty()) return;
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE discovered_resource SET last_seen_at=greatest(last_seen_at,?),state='active' WHERE info_hash=?")) {
      connection.setAutoCommit(false); int pending = 0;
      try {
        for (var observation : observations.entrySet()) {
          statement.setTimestamp(1, Timestamp.from(observation.getValue())); statement.setString(2, observation.getKey()); statement.addBatch();
          if (++pending == TOUCH_BATCH_SIZE) { statement.executeBatch(); statement.clearBatch(); pending = 0; }
        }
        if (pending > 0) statement.executeBatch(); connection.commit();
      } catch (SQLException error) { connection.rollback(); throw error; }
      finally { connection.setAutoCommit(true); }
    }
  }

  /** Persist a coalesced DHT intake batch in one transaction and one pooled connection. */
  ObservationWrite persistObservations(List<DhtObservation> observations) throws SQLException {
    if (observations == null || observations.isEmpty()) {
      return new ObservationWrite(Set.of(), Set.of());
    }
    var fresh = new java.util.LinkedHashSet<String>();
    var immediate = new java.util.LinkedHashSet<String>();
    int peerFacts = 0;
    int factCount = 0;
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        int[] inserted;
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO discovered_resource(info_hash,first_seen_at,last_seen_at,source,state) "
                + "VALUES(?,?,?,?,'active') ON CONFLICT(info_hash) DO NOTHING")) {
          for (DhtObservation observation : observations) {
            Timestamp at = Timestamp.from(observation.observedAt());
            statement.setString(1, observation.infoHash());
            statement.setTimestamp(2, at);
            statement.setTimestamp(3, at);
            statement.setString(4, observation.query());
            statement.addBatch();
          }
          inserted = statement.executeBatch();
        }
        for (int index = 0; index < inserted.length; index++) {
          if (statementChanged(inserted[index])) fresh.add(observations.get(index).infoHash());
        }

        try (PreparedStatement statement = connection.prepareStatement(
            "UPDATE discovered_resource SET last_seen_at=greatest(last_seen_at,?),state='active' "
                + "WHERE info_hash=?")) {
          for (DhtObservation observation : observations) {
            statement.setTimestamp(1, Timestamp.from(observation.observedAt()));
            statement.setString(2, observation.infoHash());
            statement.addBatch();
          }
          statement.executeBatch();
        }

        int[] queued;
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO metadata_job(info_hash,priority,attempts,next_attempt_at,updated_at) "
                + "SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM content WHERE info_hash=?) "
                + "ON CONFLICT(info_hash) DO UPDATE SET priority=greatest(metadata_job.priority,excluded.priority),"
                + "next_attempt_at=CASE WHEN ? THEN least(metadata_job.next_attempt_at,excluded.next_attempt_at) "
                + "ELSE metadata_job.next_attempt_at END,updated_at=excluded.updated_at")) {
          for (DhtObservation observation : observations) {
            int priority = observation.isAnnounce() ? 100 : 10;
            Timestamp at = Timestamp.from(observation.observedAt());
            statement.setString(1, observation.infoHash());
            statement.setInt(2, priority);
            statement.setInt(3, 0);
            statement.setTimestamp(4, at);
            statement.setTimestamp(5, at);
            statement.setString(6, observation.infoHash());
            statement.setBoolean(7, observation.isAnnounce());
            statement.addBatch();
          }
          queued = statement.executeBatch();
        }
        for (int index = 0; index < queued.length; index++) {
          DhtObservation observation = observations.get(index);
          if (observation.isAnnounce() && statementChanged(queued[index])) {
            immediate.add(observation.infoHash());
          }
        }

        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,peer_host,peer_port,"
                + "source_host,source_port,mode,raw_event) VALUES(?,?,?,?,?,?,?,?,?,?::jsonb) "
                + "ON CONFLICT DO NOTHING")) {
          for (DhtObservation observation : observations) {
            if (fresh.contains(observation.infoHash())) {
              bindObservationFact(statement, "dht.resource_discovered", observation,
                  null, resourceObservationJson(observation));
              statement.addBatch();
              factCount++;
            }
            if (observation.isAnnounce() && observation.peer() != null) {
              bindObservationFact(statement, "dht.peer_discovered", observation,
                  observation.peer(), peerObservationJson(observation));
              statement.addBatch();
              factCount++;
              peerFacts++;
            }
          }
          if (factCount > 0) statement.executeBatch();
        }
        connection.commit();
      } catch (SQLException error) {
        connection.rollback();
        throw error;
      } finally {
        connection.setAutoCommit(true);
      }
    }
    queueCounter("discovered", fresh.size());
    queueCounter("probes", factCount);
    queueCounter("peers", peerFacts);
    return new ObservationWrite(Set.copyOf(fresh), Set.copyOf(immediate));
  }

  private static boolean statementChanged(int count) {
    return count > 0 || count == Statement.SUCCESS_NO_INFO;
  }

  private static void bindObservationFact(PreparedStatement statement, String type,
                                          DhtObservation observation, InetSocketAddress peer,
                                          String rawJson) throws SQLException {
    InetSocketAddress source = observation.source();
    statement.setString(1, UUID.randomUUID().toString());
    statement.setString(2, type);
    statement.setTimestamp(3, Timestamp.from(observation.observedAt()));
    statement.setString(4, observation.infoHash());
    statement.setString(5, peer == null ? null : peer.getHostString());
    setNullableInteger(statement, 6, peer == null ? null : peer.getPort());
    statement.setString(7, source == null ? null : source.getHostString());
    setNullableInteger(statement, 8, source == null ? null : source.getPort());
    statement.setString(9, "passive");
    statement.setString(10, rawJson);
  }

  private static String resourceObservationJson(DhtObservation observation) {
    return "{\"event\":\"dht.resource_discovered\",\"info_hash\":\""
        + observation.infoHash() + "\",\"query\":\"" + observation.query() + "\"}";
  }

  private static String peerObservationJson(DhtObservation observation) {
    InetSocketAddress peer = observation.peer();
    StringBuilder json = new StringBuilder("{\"event\":\"dht.peer_discovered\",\"info_hash\":\"")
        .append(observation.infoHash()).append("\",\"peer\":{\"host\":\"")
        .append(jsonEscape(peer.getHostString())).append("\",\"port\":").append(peer.getPort()).append('}');
    InetSocketAddress source = observation.source();
    if (source != null) {
      json.append(",\"discovered_from\":{\"host\":\"")
          .append(jsonEscape(source.getHostString())).append("\",\"port\":")
          .append(source.getPort()).append('}');
    }
    return json.append('}').toString();
  }

  private static String jsonEscape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r");
  }

  void event(String type, String hash, String rawJson) throws SQLException { event(type, hash, rawJson, "passive"); }
  void event(String type, String hash, String rawJson, String mode) throws SQLException {
    event(type, hash, rawJson, mode, null, null);
  }
  void event(String type, String hash, String rawJson, String mode,
             InetSocketAddress peer, InetSocketAddress source) throws SQLException {
    Instant now = Instant.now();
    boolean inserted;
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement("INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,raw_event) VALUES(?,?,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING")) {
        statement.setString(1, UUID.randomUUID().toString()); statement.setString(2, type); statement.setTimestamp(3, Timestamp.from(now)); statement.setString(4, hash);
        statement.setString(5, peer == null ? null : peer.getHostString());
        setNullableInteger(statement, 6, peer == null ? null : peer.getPort());
        statement.setString(7, source == null ? null : source.getHostString());
        setNullableInteger(statement, 8, source == null ? null : source.getPort());
        statement.setString(9, mode); statement.setString(10, rawJson);
        inserted = statement.executeUpdate() > 0;
      }
      connection.commit();
    }
    if (inserted) queueEventCounters(type);
  }

  void ingestEvent(Map<String,Object> event, String rawJson) throws SQLException {
    String eventId = string(event.get("event_id"));
    String type = string(event.get("event"));
    String occurred = string(event.get("occurred_at"));
    if (eventId.isBlank() || type.isBlank() || occurred.isBlank()) return;
    Map<?,?> peer = event.get("peer") instanceof Map<?,?> value ? value : Map.of();
    Map<?,?> source = event.get("discovered_from") instanceof Map<?,?> value ? value : Map.of();
    boolean inserted;
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message,raw_event) VALUES(?,?,?::timestamptz,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING")) {
          statement.setString(1, eventId); statement.setString(2, type); statement.setString(3, occurred);
          statement.setString(4, nullable(event.get("info_hash"))); statement.setString(5, nullable(peer.get("host")));
          setNullableInteger(statement, 6, peer.get("port")); statement.setString(7, nullable(source.get("host")));
          setNullableInteger(statement, 8, source.get("port")); statement.setString(9, nullable(event.get("mode")));
          statement.setString(10, nullable(event.get("message"))); statement.setString(11, rawJson);
          inserted = statement.executeUpdate() > 0;
        }
        connection.commit();
      } catch (Exception error) {
        connection.rollback();
        if (error instanceof SQLException sql) throw sql;
        throw new SQLException("invalid event", error);
      } finally { connection.setAutoCommit(true); }
    }
    if (inserted) queueEventCounters(type);
  }

  private static void setNullableInteger(PreparedStatement statement, int index, Object value) throws SQLException {
    if (value instanceof Number number) statement.setInt(index, number.intValue());
    else if (value != null && !String.valueOf(value).isBlank()) statement.setInt(index, Integer.parseInt(String.valueOf(value)));
    else statement.setNull(index, java.sql.Types.INTEGER);
  }

  private static String string(Object value) { return value == null ? "" : String.valueOf(value); }
  private static String nullable(Object value) { String result = string(value); return result.isBlank() ? null : result; }

  private void queueEventCounters(String type) {
    queueCounter("probes", 1);
    if (type.equals("dht.peer_discovered")) queueCounter("peers", 1);
    if (type.equals("dht.lookup_completed")) queueCounter("lookups", 1);
  }

  private void queueCounter(String name, long amount) {
    if (amount == 0) return;
    pendingCounters.computeIfAbsent(name, ignored -> new AtomicLong()).addAndGet(amount);
  }

  /** Flush counter deltas in one short transaction instead of locking a hot row per event. */
  void flushCounters() throws SQLException {
    Map<String, Long> deltas = new LinkedHashMap<>();
    pendingCounters.forEach((name, value) -> {
      long delta = value.getAndSet(0);
      if (delta != 0) deltas.put(name, delta);
    });
    if (deltas.isEmpty()) return;
    try {
      try (Connection connection = dataSource.getConnection();
           PreparedStatement statement = connection.prepareStatement("INSERT INTO catalog_counter(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=catalog_counter.value+excluded.value")) {
        connection.setAutoCommit(false);
        try {
          for (var entry : deltas.entrySet()) {
            statement.setString(1, entry.getKey());
            statement.setLong(2, entry.getValue());
            statement.addBatch();
          }
          statement.executeBatch();
          connection.commit();
        } catch (SQLException error) {
          connection.rollback();
          throw error;
        } finally {
          connection.setAutoCommit(true);
        }
      }
    } catch (SQLException error) {
      deltas.forEach((name, delta) -> pendingCounters.computeIfAbsent(name, ignored -> new AtomicLong()).addAndGet(delta));
      throw error;
    }
  }

  void aggregateMonitorBatch(List<MonitorPoint> points) throws SQLException {
    if (points == null || points.isEmpty()) return;
    Map<Instant, long[]> buckets = new TreeMap<>();
    for (MonitorPoint point : points) {
      long[] delta = point.delta();
      if (delta[0] + delta[1] + delta[2] + delta[3] + delta[4] == 0) continue;
      Instant bucket = point.occurredAt().truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
      long[] total = buckets.computeIfAbsent(bucket, ignored -> new long[5]);
      for (int index = 0; index < total.length; index++) total[index] += delta[index];
    }
    if (buckets.isEmpty()) return;
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        try (PreparedStatement metric = connection.prepareStatement(
            "INSERT INTO minute_metric(bucket,links,queries,failures,warnings,indexed) VALUES(?,?,?,?,?,?) ON CONFLICT(bucket) DO UPDATE SET links=minute_metric.links+excluded.links,queries=minute_metric.queries+excluded.queries,failures=minute_metric.failures+excluded.failures,warnings=minute_metric.warnings+excluded.warnings,indexed=minute_metric.indexed+excluded.indexed")) {
          for (var entry : buckets.entrySet()) {
            metric.setTimestamp(1, Timestamp.from(entry.getKey()));
            long[] values = entry.getValue();
            for (int index = 0; index < values.length; index++) metric.setLong(index + 2, values[index]);
            metric.addBatch();
          }
          metric.executeBatch();
        }
        connection.commit();
      } catch (SQLException error) {
        connection.rollback();
        throw error;
      }
    }
  }

  boolean queueMetadataJob(String hash, Instant at, int priority, boolean accelerate) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("INSERT INTO metadata_job(info_hash,priority,attempts,next_attempt_at,updated_at) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM content WHERE info_hash=?) ON CONFLICT(info_hash) DO UPDATE SET priority=greatest(metadata_job.priority,excluded.priority),next_attempt_at=CASE WHEN ? THEN least(metadata_job.next_attempt_at,excluded.next_attempt_at) ELSE metadata_job.next_attempt_at END,updated_at=excluded.updated_at")) {
      statement.setString(1, hash); statement.setInt(2, priority); statement.setInt(3, 0); statement.setTimestamp(4, Timestamp.from(at)); statement.setTimestamp(5, Timestamp.from(at)); statement.setString(6, hash); statement.setBoolean(7, accelerate); return statement.executeUpdate() > 0;
    }
  }

  boolean claimImmediateMetadataJob(String hash, Instant at) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE metadata_job j SET attempts=j.attempts+1,priority=0,next_attempt_at=?::timestamptz + interval '10 minutes',updated_at=?::timestamptz WHERE j.info_hash=? AND j.next_attempt_at <= ?::timestamptz AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash=j.info_hash) RETURNING j.info_hash")) {
      Timestamp timestamp = Timestamp.from(at);
      statement.setTimestamp(1, timestamp); statement.setTimestamp(2, timestamp); statement.setString(3, hash); statement.setTimestamp(4, timestamp);
      try (ResultSet rows = statement.executeQuery()) { return rows.next(); }
    }
  }

  List<String> claimMetadataJobs(int limit, Instant at) throws SQLException {
    return claimMetadataJobs(limit, at, 0);
  }

  List<String> claimMetadataJobs(int limit, Instant at, int minimumPriority) throws SQLException {
    return claimMetadataJobs(limit, at, minimumPriority, Integer.MAX_VALUE);
  }

  List<String> claimMetadataJobs(int limit, Instant at, int minimumPriority, int maximumPriority) throws SQLException {
    List<String> result = new ArrayList<>();
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("WITH due AS (SELECT j.info_hash FROM metadata_job j WHERE j.next_attempt_at <= ?::timestamptz AND j.priority >= ? AND j.priority < ? AND EXISTS (SELECT 1 FROM discovered_resource r WHERE r.info_hash=j.info_hash AND r.state='active' AND r.last_seen_at >= ?::timestamptz) AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash=j.info_hash) ORDER BY j.priority DESC,j.updated_at DESC,j.next_attempt_at ASC FOR UPDATE OF j SKIP LOCKED LIMIT ?) UPDATE metadata_job j SET attempts=j.attempts+1,priority=0,next_attempt_at=?::timestamptz + interval '10 minutes',updated_at=?::timestamptz FROM due WHERE j.info_hash=due.info_hash RETURNING j.info_hash")) {
      Timestamp timestamp = Timestamp.from(at); Timestamp recent = Timestamp.from(at.minus(java.time.Duration.ofHours(24))); statement.setTimestamp(1, timestamp); statement.setInt(2, minimumPriority); statement.setInt(3, maximumPriority); statement.setTimestamp(4, recent); statement.setInt(5, limit); statement.setTimestamp(6, timestamp); statement.setTimestamp(7, timestamp);
      try (ResultSet rows = statement.executeQuery()) { while (rows.next()) result.add(rows.getString(1)); }
    }
    return result;
  }

  /** Recover recently announced peers in one indexed query for a claimed job batch. */
  Map<String, List<InetSocketAddress>> recentPeerHints(Collection<String> hashes, Instant cutoff,
                                                        int eventsPerHash) throws SQLException {
    if (hashes == null || hashes.isEmpty() || eventsPerHash < 1) return Map.of();
    Map<String, List<InetSocketAddress>> result = new LinkedHashMap<>();
    String sql = "SELECT requested.info_hash,e.peer_host,e.peer_port,e.source_host,e.source_port "
        + "FROM unnest(?::text[]) requested(info_hash) JOIN LATERAL ("
        + "SELECT peer_host,peer_port,source_host,source_port FROM probe_event "
        + "WHERE info_hash=requested.info_hash AND event_type='dht.peer_discovered' "
        + "AND occurred_at>=?::timestamptz AND peer_host IS NOT NULL "
        + "AND peer_port BETWEEN 1 AND 65535 ORDER BY occurred_at DESC LIMIT ?) e ON true";
    try (Connection connection = dataSource.getConnection();
         PreparedStatement statement = connection.prepareStatement(sql)) {
      java.sql.Array hashArray = connection.createArrayOf("text", hashes.toArray(String[]::new));
      try {
        statement.setArray(1, hashArray);
        statement.setTimestamp(2, Timestamp.from(cutoff));
        statement.setInt(3, eventsPerHash);
        statement.setQueryTimeout(3);
        try (ResultSet rows = statement.executeQuery()) {
          while (rows.next()) {
            String hash = rows.getString(1);
            addPeerHint(result, hash, rows.getString(2), rows.getObject(3, Integer.class));
            addPeerHint(result, hash, rows.getString(4), rows.getObject(5, Integer.class));
          }
        }
      } finally {
        hashArray.free();
      }
    }
    result.replaceAll((ignored, peers) -> List.copyOf(peers));
    return Map.copyOf(result);
  }

  private static void addPeerHint(Map<String, List<InetSocketAddress>> target, String hash,
                                  String host, Integer port) {
    if (hash == null || host == null || host.isBlank() || port == null || port < 1 || port > 65535) return;
    InetSocketAddress endpoint = new InetSocketAddress(host, port);
    if (endpoint.isUnresolved()) return;
    List<InetSocketAddress> peers = target.computeIfAbsent(hash, ignored -> new ArrayList<>());
    if (!peers.contains(endpoint)) peers.add(endpoint);
  }

  void completeMetadataJob(String hash, boolean succeeded, Instant at) throws SQLException {
    if (succeeded) {
      try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("DELETE FROM metadata_job WHERE info_hash=?")) { statement.setString(1, hash); statement.executeUpdate(); }
      return;
    }
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE metadata_job SET next_attempt_at=CASE WHEN priority>=100 THEN least(next_attempt_at,?::timestamptz) ELSE ?::timestamptz + make_interval(hours => least(168,power(2,least(greatest(attempts-1,0),8))::integer)) END,updated_at=?::timestamptz WHERE info_hash=?")) { Timestamp timestamp = Timestamp.from(at); statement.setTimestamp(1, timestamp); statement.setTimestamp(2, timestamp); statement.setTimestamp(3, timestamp); statement.setString(4, hash); statement.executeUpdate(); }
  }

  int markInvalidResources(Instant cutoff) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE discovered_resource SET state='invalid' WHERE state!='invalid' AND last_seen_at < ?")) {
      statement.setTimestamp(1, Timestamp.from(cutoff));
      return statement.executeUpdate();
    }
  }

  ManifestWrite upsertManifest(Manifest manifest) throws SQLException {
    String contentId = "btih:" + manifest.infoHash();
    Instant now = Instant.now();
    boolean inserted = false;
    boolean changed = false;
    int fileDelta = 0;
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO content(content_id,info_hash,variant,name,total_size,file_count,metadata_size,metadata_sha256,policy_state,files_text,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'approved',?,?,?) ON CONFLICT(content_id) DO UPDATE SET variant=excluded.variant,name=excluded.name,total_size=excluded.total_size,file_count=excluded.file_count,metadata_size=excluded.metadata_size,metadata_sha256=excluded.metadata_sha256,files_text=excluded.files_text,updated_at=excluded.updated_at WHERE content.variant IS DISTINCT FROM excluded.variant OR content.name IS DISTINCT FROM excluded.name OR content.total_size IS DISTINCT FROM excluded.total_size OR content.file_count IS DISTINCT FROM excluded.file_count OR content.metadata_size IS DISTINCT FROM excluded.metadata_size OR content.metadata_sha256 IS DISTINCT FROM excluded.metadata_sha256 OR content.files_text IS DISTINCT FROM excluded.files_text RETURNING (xmax=0)")) {
          statement.setString(1, contentId); statement.setString(2, manifest.infoHash()); statement.setString(3, manifest.variant()); statement.setString(4, manifest.name()); statement.setLong(5, manifest.totalSize()); statement.setInt(6, manifest.fileCount()); statement.setInt(7, manifest.metadataSize()); statement.setString(8, manifest.metadataSha256()); statement.setString(9, manifest.files().stream().map(ManifestFile::path).reduce((a,b)->a+" "+b).orElse("")); statement.setTimestamp(10, Timestamp.from(now)); statement.setTimestamp(11, Timestamp.from(now));
          try (ResultSet rows = statement.executeQuery()) {
            if (!rows.next()) { connection.commit(); return new ManifestWrite(false, false); }
            inserted = rows.getBoolean(1);
            changed = true;
          }
        }
        int oldFiles;
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM file_entry WHERE content_id=?")) { statement.setString(1, contentId); oldFiles = statement.executeUpdate(); }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO file_entry(content_id,ordinal,path,size) VALUES(?,?,?,?)")) { int ordinal=0; for (ManifestFile file : manifest.files()) { statement.setString(1, contentId); statement.setInt(2, ordinal++); statement.setString(3, file.path()); statement.setLong(4, file.size()); statement.addBatch(); } statement.executeBatch(); }
        fileDelta = manifest.files().size() - oldFiles;
        connection.commit();
      } catch (SQLException error) { connection.rollback(); throw error; } finally { connection.setAutoCommit(true); }
    }
    if (fileDelta != 0) queueCounter("files", fileDelta);
    if (inserted) queueCounter("content", 1);
    return new ManifestWrite(inserted, changed);
  }

  Map<String,Object> dashboardSummary() throws SQLException {
    Map<String,Object> result = new LinkedHashMap<>();
    try (Connection connection=dataSource.getConnection(); Statement statement=connection.createStatement(); ResultSet rows=statement.executeQuery("SELECT coalesce((SELECT value FROM catalog_counter WHERE name='content'),0),coalesce((SELECT value FROM catalog_counter WHERE name='files'),0),coalesce((SELECT value FROM catalog_counter WHERE name='probes'),0),coalesce((SELECT value FROM catalog_counter WHERE name='peers'),0),coalesce((SELECT value FROM catalog_counter WHERE name='lookups'),0),coalesce((SELECT value FROM catalog_counter WHERE name='discovered'),0),coalesce((SELECT count(*) FROM discovered_resource WHERE state='active'),0),coalesce((SELECT count(*) FROM discovered_resource WHERE state='invalid'),0),(SELECT occurred_at FROM probe_event ORDER BY occurred_at DESC LIMIT 1)")) { if(rows.next()){ String[] keys={"content","files","probes","peers","lookups","discovered","active_discovered","invalid_discovered"}; for(int i=0;i<keys.length;i++) result.put(keys[i],rows.getLong(i+1)); result.put("last_event_at",rows.getTimestamp(9)==null?null:rows.getTimestamp(9).toInstant().toString()); } }
    return result;
  }

  List<Map<String,Object>> recentProbes(int limit) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement("SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message FROM probe_event ORDER BY occurred_at DESC LIMIT ?")){p.setInt(1,limit);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>(); row.put("event_id",r.getString(1));row.put("event_type",r.getString(2));row.put("occurred_at",r.getTimestamp(3).toInstant().toString());row.put("info_hash",r.getString(4));row.put("peer_host",r.getString(5));row.put("peer_port",r.getObject(6));row.put("source_host",r.getString(7));row.put("source_port",r.getObject(8));row.put("mode",r.getString(9));row.put("message",r.getString(10));result.add(row);}}}return result; }
  List<Map<String,Object>> contentPage(int limit,int offset) throws SQLException { return contentQuery("SELECT content_id,info_hash,variant,name,total_size,file_count,created_at,updated_at FROM content WHERE policy_state='approved' ORDER BY updated_at DESC,content_id DESC LIMIT ? OFFSET ?",limit,offset,null); }
  List<Map<String,Object>> searchPage(String query,int limit,int offset) throws SQLException {
    String term=validatedSearch(query);
    return contentQuery("WITH search AS (SELECT plainto_tsquery('simple', ?) AS term) SELECT c.content_id,c.info_hash,c.variant,c.name,c.total_size,c.file_count,c.created_at,c.updated_at FROM content c,search s WHERE c.policy_state='approved' AND " + CONTENT_SEARCH_MATCH + " ORDER BY CASE WHEN c.name ILIKE ? THEN 0 ELSE 1 END,c.updated_at DESC,c.content_id DESC LIMIT ? OFFSET ?",limit,offset,term);
  }
  private List<Map<String,Object>> contentQuery(String sql,int limit,int offset,String term) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)){p.setQueryTimeout(5);int i=1;if(term!=null){p.setString(i++,term);p.setString(i++,"%"+term+"%");}p.setInt(i++,limit);p.setInt(i,offset);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();row.put("content_id",r.getString(1));row.put("info_hash",r.getString(2));row.put("variant",r.getString(3));row.put("name",r.getString(4));row.put("total_size",r.getLong(5));row.put("file_count",r.getInt(6));row.put("created_at",r.getTimestamp(7).toInstant().toString());row.put("updated_at",r.getTimestamp(8).toInstant().toString());result.add(row);}}}return result; }
  long contentCount(String query) throws SQLException { if (query == null) return counter("content"); String term=validatedSearch(query); String sql="WITH search AS (SELECT plainto_tsquery('simple', ?) AS term) SELECT count(*) FROM content c,search s WHERE c.policy_state='approved' AND " + CONTENT_SEARCH_MATCH;try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)){p.setQueryTimeout(5);p.setString(1,term);try(ResultSet r=p.executeQuery()){return r.next()?r.getLong(1):0;}} }
  static String validatedSearch(String query) { String trimmed=query==null?"":query.trim(); int length=trimmed.codePointCount(0,trimmed.length()); if(length<3||length>100) throw new IllegalArgumentException("search query must contain 3 to 100 characters"); return trimmed; }
  private long counter(String name) throws SQLException { try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement("SELECT value FROM catalog_counter WHERE name=?")){p.setString(1,name);try(ResultSet r=p.executeQuery()){return r.next()?r.getLong(1):0;}} }
  Connection connection() throws SQLException { return dataSource.getConnection(); }
  List<Map<String,Object>> trend(int minutes) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement("WITH bounds AS (SELECT date_trunc('minute',now())-interval '1 minute' AS last_complete_bucket), buckets AS (SELECT generate_series(last_complete_bucket-(?::integer-1)*interval '1 minute',last_complete_bucket,interval '1 minute') AS bucket FROM bounds) SELECT b.bucket,coalesce(m.links,0),coalesce(m.queries,0),coalesce(m.failures,0),coalesce(m.warnings,0),coalesce(m.indexed,0) FROM buckets b LEFT JOIN minute_metric m USING(bucket) ORDER BY b.bucket")){p.setInt(1,minutes);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();row.put("at",r.getTimestamp(1).toInstant().toString());row.put("links",r.getLong(2));row.put("queries",r.getLong(3));row.put("failures",r.getLong(4));row.put("warnings",r.getLong(5));row.put("indexed",r.getLong(6));result.add(row);}}}return result; }
  @Override public void close(){
    try { flushCounters(); }
    catch (SQLException error) { System.err.println("catalog counter flush failed: " + error.getMessage()); }
    dataSource.close();
  }
}
