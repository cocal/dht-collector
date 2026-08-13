package com.cocal.dht;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

final class Catalog implements AutoCloseable {
  private static final int TOUCH_BATCH_SIZE = 1_000;
  private final HikariDataSource dataSource;

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
      statement.executeUpdate("CREATE TABLE IF NOT EXISTS minute_metric (bucket timestamptz PRIMARY KEY, links bigint NOT NULL DEFAULT 0, queries bigint NOT NULL DEFAULT 0, failures bigint NOT NULL DEFAULT 0, warnings bigint NOT NULL DEFAULT 0)");
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
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("WITH upsert AS (INSERT INTO discovered_resource(info_hash,first_seen_at,last_seen_at,source,state) VALUES(?,?,?,?,'active') ON CONFLICT(info_hash) DO UPDATE SET last_seen_at=greatest(discovered_resource.last_seen_at,excluded.last_seen_at),state='active' RETURNING (xmax=0) AS fresh), counter AS (INSERT INTO catalog_counter(name,value) SELECT 'discovered',count(*) FROM upsert WHERE fresh ON CONFLICT(name) DO UPDATE SET value=catalog_counter.value+excluded.value) SELECT fresh FROM upsert")) {
      statement.setString(1, hash); statement.setTimestamp(2, Timestamp.from(at)); statement.setTimestamp(3, Timestamp.from(at)); statement.setString(4, source);
      try (ResultSet rows = statement.executeQuery()) { return rows.next() && rows.getBoolean(1); }
    }
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

  void event(String type, String hash, String rawJson) throws SQLException { event(type, hash, rawJson, "passive"); }
  void event(String type, String hash, String rawJson, String mode) throws SQLException {
    Instant now = Instant.now();
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement statement = connection.prepareStatement("INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,mode,raw_event) VALUES(?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING")) {
        statement.setString(1, UUID.randomUUID().toString()); statement.setString(2, type); statement.setTimestamp(3, Timestamp.from(now)); statement.setString(4, hash); statement.setString(5, mode); statement.setString(6, rawJson);
        if (statement.executeUpdate() > 0) {
          incrementCounter(connection, "probes", 1);
          if (type.equals("dht.peer_discovered")) incrementCounter(connection, "peers", 1);
          if (type.equals("dht.lookup_completed")) incrementCounter(connection, "lookups", 1);
          long links = type.equals("dht.resource_discovered") ? 1 : 0;
          long queries = type.equals("dht.query_summary") ? jsonLong(rawJson, "occurrences") : 0;
          long failures = type.endsWith("failed") || type.equals("dht.error") ? 1 : 0;
          long warnings = type.equals("dht.warning") ? 1 : 0;
          if (links + queries + failures + warnings > 0) {
            try (PreparedStatement metric = connection.prepareStatement("INSERT INTO minute_metric(bucket,links,queries,failures,warnings) VALUES(date_trunc('minute',?::timestamptz),?,?,?,?) ON CONFLICT(bucket) DO UPDATE SET links=minute_metric.links+excluded.links,queries=minute_metric.queries+excluded.queries,failures=minute_metric.failures+excluded.failures,warnings=minute_metric.warnings+excluded.warnings")) {
              metric.setTimestamp(1, Timestamp.from(now)); metric.setLong(2, links); metric.setLong(3, queries); metric.setLong(4, failures); metric.setLong(5, warnings); metric.executeUpdate();
            }
          }
        }
      }
      connection.commit();
    }
  }

  void ingestEvent(Map<String,Object> event, String rawJson) throws SQLException {
    String eventId = string(event.get("event_id"));
    String type = string(event.get("event"));
    String occurred = string(event.get("occurred_at"));
    if (eventId.isBlank() || type.isBlank() || occurred.isBlank()) return;
    Map<?,?> peer = event.get("peer") instanceof Map<?,?> value ? value : Map.of();
    Map<?,?> source = event.get("discovered_from") instanceof Map<?,?> value ? value : Map.of();
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        int inserted;
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO probe_event(event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message,raw_event) VALUES(?,?,?::timestamptz,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING")) {
          statement.setString(1, eventId); statement.setString(2, type); statement.setString(3, occurred);
          statement.setString(4, nullable(event.get("info_hash"))); statement.setString(5, nullable(peer.get("host")));
          setNullableInteger(statement, 6, peer.get("port")); statement.setString(7, nullable(source.get("host")));
          setNullableInteger(statement, 8, source.get("port")); statement.setString(9, nullable(event.get("mode")));
          statement.setString(10, nullable(event.get("message"))); statement.setString(11, rawJson);
          inserted = statement.executeUpdate();
        }
        if (inserted > 0) updateEventAggregates(connection, type, Instant.parse(occurred), rawJson);
        connection.commit();
      } catch (Exception error) {
        connection.rollback();
        if (error instanceof SQLException sql) throw sql;
        throw new SQLException("invalid event", error);
      } finally { connection.setAutoCommit(true); }
    }
  }

  private static void setNullableInteger(PreparedStatement statement, int index, Object value) throws SQLException {
    if (value instanceof Number number) statement.setInt(index, number.intValue());
    else if (value != null && !String.valueOf(value).isBlank()) statement.setInt(index, Integer.parseInt(String.valueOf(value)));
    else statement.setNull(index, java.sql.Types.INTEGER);
  }

  private static String string(Object value) { return value == null ? "" : String.valueOf(value); }
  private static String nullable(Object value) { String result = string(value); return result.isBlank() ? null : result; }

  private static void updateEventAggregates(Connection connection, String type, Instant occurredAt, String rawJson) throws SQLException {
    incrementCounter(connection, "probes", 1);
    if (type.equals("dht.peer_discovered")) incrementCounter(connection, "peers", 1);
    if (type.equals("dht.lookup_completed")) incrementCounter(connection, "lookups", 1);
    long links = type.equals("dht.resource_discovered") ? 1 : 0;
    long queries = type.equals("dht.query_summary") ? jsonLong(rawJson, "occurrences") : type.equals("dht.query_received") ? 1 : 0;
    long failures = type.equals("metadata.fetch_summary") ? jsonLong(rawJson, "failures")
        : type.endsWith("failed") || type.endsWith("_failed") || type.equals("dht.error") ? 1 : 0;
    long warnings = type.equals("dht.warning") ? jsonLong(rawJson, "occurrences") : 0;
    if (links + queries + failures + warnings == 0) return;
    try (PreparedStatement metric = connection.prepareStatement("INSERT INTO minute_metric(bucket,links,queries,failures,warnings) VALUES(date_trunc('minute',?::timestamptz),?,?,?,?) ON CONFLICT(bucket) DO UPDATE SET links=minute_metric.links+excluded.links,queries=minute_metric.queries+excluded.queries,failures=minute_metric.failures+excluded.failures,warnings=minute_metric.warnings+excluded.warnings")) {
      metric.setTimestamp(1, Timestamp.from(occurredAt)); metric.setLong(2, links); metric.setLong(3, queries);
      metric.setLong(4, failures); metric.setLong(5, warnings); metric.executeUpdate();
    }
  }

  private static long jsonLong(String json, String field) {
    String marker = "\"" + field + "\":"; int start = json.indexOf(marker); if (start < 0) return 1; start += marker.length(); int end = start;
    while (end < json.length() && Character.isDigit(json.charAt(end))) end++;
    try { return Long.parseLong(json.substring(start, end)); } catch (Exception ignored) { return 1; }
  }

  boolean queueMetadataJob(String hash, Instant at, int priority, boolean accelerate) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("INSERT INTO metadata_job(info_hash,priority,attempts,next_attempt_at,updated_at) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM content WHERE info_hash=?) ON CONFLICT(info_hash) DO UPDATE SET priority=greatest(metadata_job.priority,excluded.priority),next_attempt_at=CASE WHEN ? THEN least(metadata_job.next_attempt_at,excluded.next_attempt_at) ELSE metadata_job.next_attempt_at END,updated_at=excluded.updated_at")) {
      statement.setString(1, hash); statement.setInt(2, priority); statement.setInt(3, 0); statement.setTimestamp(4, Timestamp.from(at)); statement.setTimestamp(5, Timestamp.from(at)); statement.setString(6, hash); statement.setBoolean(7, accelerate); return statement.executeUpdate() > 0;
    }
  }

  List<String> claimMetadataJobs(int limit, Instant at) throws SQLException {
    List<String> result = new ArrayList<>();
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("WITH due AS (SELECT j.info_hash FROM metadata_job j WHERE j.next_attempt_at <= ?::timestamptz AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash=j.info_hash) ORDER BY j.priority DESC,j.next_attempt_at ASC,j.updated_at DESC FOR UPDATE OF j SKIP LOCKED LIMIT ?) UPDATE metadata_job j SET attempts=j.attempts+1,priority=0,next_attempt_at=?::timestamptz + interval '10 minutes',updated_at=?::timestamptz FROM due WHERE j.info_hash=due.info_hash RETURNING j.info_hash")) {
      Timestamp timestamp = Timestamp.from(at); statement.setTimestamp(1, timestamp); statement.setInt(2, limit); statement.setTimestamp(3, timestamp); statement.setTimestamp(4, timestamp);
      try (ResultSet rows = statement.executeQuery()) { while (rows.next()) result.add(rows.getString(1)); }
    }
    return result;
  }

  void completeMetadataJob(String hash, boolean succeeded, Instant at) throws SQLException {
    if (succeeded) {
      try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("DELETE FROM metadata_job WHERE info_hash=?")) { statement.setString(1, hash); statement.executeUpdate(); }
      return;
    }
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE metadata_job SET next_attempt_at=?::timestamptz + make_interval(hours => least(168,power(2,least(greatest(attempts-1,0),8))::integer)),updated_at=?::timestamptz WHERE info_hash=?")) { Timestamp timestamp = Timestamp.from(at); statement.setTimestamp(1, timestamp); statement.setTimestamp(2, timestamp); statement.setString(3, hash); statement.executeUpdate(); }
  }

  int markInvalidResources(Instant cutoff) throws SQLException {
    try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("UPDATE discovered_resource SET state='invalid' WHERE state!='invalid' AND last_seen_at < ?")) {
      statement.setTimestamp(1, Timestamp.from(cutoff));
      return statement.executeUpdate();
    }
  }

  void upsertManifest(Manifest manifest) throws SQLException {
    String contentId = "btih:" + manifest.infoHash();
    Instant now = Instant.now();
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        boolean inserted;
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO content(content_id,info_hash,variant,name,total_size,file_count,metadata_size,metadata_sha256,policy_state,files_text,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'approved',?,?,?) ON CONFLICT(content_id) DO UPDATE SET variant=excluded.variant,name=excluded.name,total_size=excluded.total_size,file_count=excluded.file_count,metadata_size=excluded.metadata_size,metadata_sha256=excluded.metadata_sha256,files_text=excluded.files_text,updated_at=excluded.updated_at RETURNING (xmax=0)")) {
          statement.setString(1, contentId); statement.setString(2, manifest.infoHash()); statement.setString(3, manifest.variant()); statement.setString(4, manifest.name()); statement.setLong(5, manifest.totalSize()); statement.setInt(6, manifest.fileCount()); statement.setInt(7, manifest.metadataSize()); statement.setString(8, manifest.metadataSha256()); statement.setString(9, manifest.files().stream().map(ManifestFile::path).reduce((a,b)->a+" "+b).orElse("")); statement.setTimestamp(10, Timestamp.from(now)); statement.setTimestamp(11, Timestamp.from(now)); try (ResultSet rows = statement.executeQuery()) { rows.next(); inserted = rows.getBoolean(1); }
        }
        int oldFiles;
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM file_entry WHERE content_id=?")) { statement.setString(1, contentId); oldFiles = statement.executeUpdate(); }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO file_entry(content_id,ordinal,path,size) VALUES(?,?,?,?)")) { int ordinal=0; for (ManifestFile file : manifest.files()) { statement.setString(1, contentId); statement.setInt(2, ordinal++); statement.setString(3, file.path()); statement.setLong(4, file.size()); statement.addBatch(); } statement.executeBatch(); }
        incrementCounter(connection, "files", manifest.files().size() - oldFiles); if (inserted) incrementCounter(connection, "content", 1); connection.commit();
      } catch (SQLException error) { connection.rollback(); throw error; } finally { connection.setAutoCommit(true); }
    }
  }

  private static void incrementCounter(Connection connection, String name, long amount) throws SQLException { if (amount == 0) return; try (PreparedStatement statement = connection.prepareStatement("INSERT INTO catalog_counter(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=catalog_counter.value+excluded.value")) { statement.setString(1,name); statement.setLong(2,amount); statement.executeUpdate(); } }

  Map<String,Object> dashboardSummary() throws SQLException {
    Map<String,Object> result = new LinkedHashMap<>();
    try (Connection connection=dataSource.getConnection(); Statement statement=connection.createStatement(); ResultSet rows=statement.executeQuery("SELECT coalesce((SELECT value FROM catalog_counter WHERE name='content'),0),coalesce((SELECT value FROM catalog_counter WHERE name='files'),0),coalesce((SELECT value FROM catalog_counter WHERE name='probes'),0),coalesce((SELECT value FROM catalog_counter WHERE name='peers'),0),coalesce((SELECT value FROM catalog_counter WHERE name='lookups'),0),coalesce((SELECT value FROM catalog_counter WHERE name='discovered'),0),coalesce((SELECT count(*) FROM discovered_resource WHERE state='active'),0),coalesce((SELECT count(*) FROM discovered_resource WHERE state='invalid'),0),(SELECT occurred_at FROM probe_event ORDER BY occurred_at DESC LIMIT 1)")) { if(rows.next()){ String[] keys={"content","files","probes","peers","lookups","discovered","active_discovered","invalid_discovered"}; for(int i=0;i<keys.length;i++) result.put(keys[i],rows.getLong(i+1)); result.put("last_event_at",rows.getTimestamp(9)==null?null:rows.getTimestamp(9).toInstant().toString()); } }
    return result;
  }

  List<Map<String,Object>> recentProbes(int limit) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement("SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message FROM probe_event ORDER BY occurred_at DESC LIMIT ?")){p.setInt(1,limit);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>(); row.put("event_id",r.getString(1));row.put("event_type",r.getString(2));row.put("occurred_at",r.getTimestamp(3).toInstant().toString());row.put("info_hash",r.getString(4));row.put("peer_host",r.getString(5));row.put("peer_port",r.getObject(6));row.put("source_host",r.getString(7));row.put("source_port",r.getObject(8));row.put("mode",r.getString(9));row.put("message",r.getString(10));result.add(row);}}}return result; }
  List<Map<String,Object>> contentPage(int limit,int offset) throws SQLException { return contentQuery("SELECT content_id,info_hash,variant,name,total_size,file_count,updated_at FROM content WHERE policy_state='approved' ORDER BY updated_at DESC LIMIT ? OFFSET ?",limit,offset,null); }
  List<Map<String,Object>> searchPage(String query,int limit,int offset) throws SQLException { String pattern="%"+query.trim()+"%"; return contentQuery("SELECT content_id,info_hash,variant,name,total_size,file_count,updated_at FROM content WHERE policy_state='approved' AND (name ILIKE ? OR files_text ILIKE ?) ORDER BY CASE WHEN name ILIKE ? THEN 0 ELSE 1 END,updated_at DESC LIMIT ? OFFSET ?",limit,offset,pattern); }
  private List<Map<String,Object>> contentQuery(String sql,int limit,int offset,String pattern) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)){int i=1;if(pattern!=null){p.setString(i++,pattern);p.setString(i++,pattern);p.setString(i++,pattern);}p.setInt(i++,limit);p.setInt(i,offset);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();row.put("content_id",r.getString(1));row.put("info_hash",r.getString(2));row.put("variant",r.getString(3));row.put("name",r.getString(4));row.put("total_size",r.getLong(5));row.put("file_count",r.getInt(6));row.put("updated_at",r.getTimestamp(7).toInstant().toString());result.add(row);}}}return result; }
  long contentCount(String query) throws SQLException { String sql=query==null?"SELECT count(*) FROM content WHERE policy_state='approved'":"SELECT count(*) FROM content WHERE policy_state='approved' AND (name ILIKE ? OR files_text ILIKE ?)";try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)){if(query!=null){p.setString(1,"%"+query.trim()+"%");p.setString(2,"%"+query.trim()+"%");}try(ResultSet r=p.executeQuery()){return r.next()?r.getLong(1):0;}} }
  Connection connection() throws SQLException { return dataSource.getConnection(); }
  List<Map<String,Object>> trend(int minutes) throws SQLException { List<Map<String,Object>> result=new ArrayList<>(); try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement("WITH bounds AS (SELECT date_trunc('minute',now()) AS current_bucket), buckets AS (SELECT generate_series(current_bucket-(?::integer-1)*interval '1 minute',current_bucket,interval '1 minute') AS bucket FROM bounds) SELECT b.bucket,coalesce(m.links,0),coalesce(m.queries,0),coalesce(m.failures,0),coalesce(m.warnings,0) FROM buckets b LEFT JOIN minute_metric m USING(bucket) ORDER BY b.bucket")){p.setInt(1,minutes);try(ResultSet r=p.executeQuery()){while(r.next()){Map<String,Object> row=new LinkedHashMap<>();row.put("at",r.getTimestamp(1).toInstant().toString());row.put("links",r.getLong(2));row.put("queries",r.getLong(3));row.put("failures",r.getLong(4));row.put("warnings",r.getLong(5));result.add(row);}}}return result; }
  @Override public void close(){dataSource.close();}
}
