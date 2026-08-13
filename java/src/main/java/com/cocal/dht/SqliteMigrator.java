package com.cocal.dht;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SqliteMigrator {
  private static final long LOCK_ID = 374810225L;
  private static final ObjectMapper JSON = new ObjectMapper();
  private SqliteMigrator() {}

  static void run(Cli.Args args, Catalog catalog) throws Exception {
    Path source = args.requiredPath("input").toAbsolutePath().normalize();
    if (!Files.isRegularFile(source)) throw new IllegalArgumentException("SQLite database not found: " + source);
    String migrationMode = args.value("migration-mode", "full");
    if (!List.of("full", "delta", "verify").contains(migrationMode)) {
      throw new IllegalArgumentException("--migration-mode must be full, delta, or verify");
    }
    catalog.initialize();
    try (Connection sqlite = java.sql.DriverManager.getConnection("jdbc:sqlite:" + source);
         Connection postgres = catalog.connection()) {
      try (Statement statement = sqlite.createStatement()) { statement.execute("PRAGMA query_only=ON"); }
      advisory(postgres, true);
      try {
        ensureState(postgres);
        if (migrationMode.equals("full")) full(sqlite, postgres, source.toString());
        else if (migrationMode.equals("delta")) delta(sqlite, postgres, source.toString());
        Map<String,Object> result = verify(sqlite, postgres);
        result.put("mode", migrationMode);
        System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
      } finally { advisory(postgres, false); }
    }
  }

  private static void advisory(Connection connection, boolean lock) throws Exception {
    try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_" + (lock ? "lock" : "unlock") + "(?)")) {
      statement.setLong(1, LOCK_ID); statement.execute();
    }
  }

  private static void ensureState(Connection postgres) throws Exception {
    try (Statement statement = postgres.createStatement()) {
      statement.executeUpdate("CREATE TABLE IF NOT EXISTS migration_state(source text PRIMARY KEY,event_rowid bigint NOT NULL,snapshot_at timestamptz NOT NULL,migrated_at timestamptz NOT NULL DEFAULT now())");
    }
  }

  private static void full(Connection sqlite, Connection postgres, String source) throws Exception {
    long eventRowId = scalar(sqlite, "SELECT coalesce(max(rowid),0) FROM probe_event");
    postgres.setAutoCommit(false);
    try {
      try (Statement statement = postgres.createStatement()) {
        statement.executeUpdate("TRUNCATE minute_metric,catalog_counter,metadata_job,file_entry,content,discovered_resource,probe_event");
      }
      copy(sqlite, postgres, "content", List.of("content_id","info_hash","variant","name","total_size","file_count","metadata_size","metadata_sha256","policy_state","created_at","updated_at"), "SELECT content_id,info_hash,variant,name,total_size,file_count,metadata_size,metadata_sha256,policy_state,created_at,updated_at FROM content");
      copy(sqlite, postgres, "file_entry", List.of("content_id","ordinal","path","size"), "SELECT content_id,ordinal,path,size FROM file_entry");
      copy(sqlite, postgres, "probe_event", List.of("event_id","event_type","occurred_at","info_hash","peer_host","peer_port","source_host","source_port","mode","message","raw_event"), "SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message,raw_event FROM probe_event WHERE rowid <= " + eventRowId);
      copy(sqlite, postgres, "discovered_resource", List.of("info_hash","first_seen_at","last_seen_at","source","state"), "SELECT info_hash,first_seen_at,last_seen_at,source,state FROM discovered_resource");
      copy(sqlite, postgres, "metadata_job", List.of("info_hash","priority","attempts","next_attempt_at","updated_at"), "SELECT info_hash,priority,attempts,next_attempt_at,updated_at FROM metadata_job");
      rebuild(postgres);
      saveState(postgres, source, eventRowId);
      postgres.commit();
    } catch (Exception error) { postgres.rollback(); throw error; }
    finally { postgres.setAutoCommit(true); }
  }

  private static void delta(Connection sqlite, Connection postgres, String source) throws Exception {
    long previous;
    try (PreparedStatement statement = postgres.prepareStatement("SELECT event_rowid FROM migration_state WHERE source=?")) {
      statement.setString(1, source);
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) throw new IllegalStateException("no full migration state found; run --migration-mode full first");
        previous = rows.getLong(1);
      }
    }
    long latest = scalar(sqlite, "SELECT coalesce(max(rowid),0) FROM probe_event");
    postgres.setAutoCommit(false);
    try {
      copy(sqlite, postgres, "probe_event", List.of("event_id","event_type","occurred_at","info_hash","peer_host","peer_port","source_host","source_port","mode","message","raw_event"),
          "SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,source_host,source_port,mode,message,raw_event FROM probe_event WHERE rowid > " + previous + " AND rowid <= " + latest, true);
      try (Statement statement = postgres.createStatement()) { statement.executeUpdate("TRUNCATE file_entry,content"); }
      copy(sqlite, postgres, "content", List.of("content_id","info_hash","variant","name","total_size","file_count","metadata_size","metadata_sha256","policy_state","created_at","updated_at"), "SELECT content_id,info_hash,variant,name,total_size,file_count,metadata_size,metadata_sha256,policy_state,created_at,updated_at FROM content");
      copy(sqlite, postgres, "file_entry", List.of("content_id","ordinal","path","size"), "SELECT content_id,ordinal,path,size FROM file_entry");
      replaceSnapshot(sqlite, postgres, "discovered_resource", List.of("info_hash","first_seen_at","last_seen_at","source","state"), "SELECT info_hash,first_seen_at,last_seen_at,source,state FROM discovered_resource");
      replaceSnapshot(sqlite, postgres, "metadata_job", List.of("info_hash","priority","attempts","next_attempt_at","updated_at"), "SELECT info_hash,priority,attempts,next_attempt_at,updated_at FROM metadata_job");
      rebuild(postgres);
      saveState(postgres, source, latest);
      postgres.commit();
    } catch (Exception error) { postgres.rollback(); throw error; }
    finally { postgres.setAutoCommit(true); }
  }

  private static void replaceSnapshot(Connection sqlite, Connection postgres, String table,
                                      List<String> columns, String query) throws Exception {
    try (Statement statement = postgres.createStatement()) { statement.executeUpdate("TRUNCATE " + table); }
    copy(sqlite, postgres, table, columns, query);
  }

  private static void copy(Connection sqlite, Connection postgres, String table,
                           List<String> columns, String query) throws Exception {
    copy(sqlite, postgres, table, columns, query, false);
  }

  private static void copy(Connection sqlite, Connection postgres, String table,
                           List<String> columns, String query, boolean ignoreConflicts) throws Exception {
    String placeholders = columns.stream().map(SqliteMigrator::placeholder).collect(java.util.stream.Collectors.joining(","));
    String sql = "INSERT INTO " + table + "(" + String.join(",", columns) + ") VALUES(" + placeholders + ")"
        + (ignoreConflicts ? " ON CONFLICT DO NOTHING" : "");
    long count = 0;
    try (Statement read = sqlite.createStatement(); ResultSet rows = read.executeQuery(query);
         PreparedStatement write = postgres.prepareStatement(sql)) {
      ResultSetMetaData metadata = rows.getMetaData();
      while (rows.next()) {
        for (int index = 1; index <= columns.size(); index++) write.setObject(index, rows.getObject(index));
        write.addBatch();
        if (++count % 1_000 == 0) write.executeBatch();
      }
      write.executeBatch();
    }
    System.out.println(table + ": " + count + " complete");
  }

  private static String placeholder(String column) {
    if (column.equals("raw_event")) return "?::jsonb";
    if (column.endsWith("_at")) return "?::timestamptz";
    return "?";
  }

  private static void rebuild(Connection postgres) throws Exception {
    try (Statement statement = postgres.createStatement()) {
      statement.executeUpdate("UPDATE content SET files_text='' ");
      statement.executeUpdate("UPDATE content c SET files_text=f.files_text FROM(SELECT content_id,string_agg(path,' ' ORDER BY ordinal) files_text FROM file_entry GROUP BY content_id)f WHERE f.content_id=c.content_id");
      statement.executeUpdate("TRUNCATE catalog_counter");
      statement.executeUpdate("INSERT INTO catalog_counter(name,value) VALUES('content',(SELECT count(*) FROM content)),('files',(SELECT count(*) FROM file_entry)),('probes',(SELECT count(*) FROM probe_event)),('peers',(SELECT count(*) FROM probe_event WHERE event_type='dht.peer_discovered')),('lookups',(SELECT count(*) FROM probe_event WHERE event_type='dht.lookup_completed')),('discovered',(SELECT count(*) FROM discovered_resource))");
      statement.executeUpdate("TRUNCATE minute_metric");
      statement.executeUpdate("INSERT INTO minute_metric(bucket,links,queries,failures,warnings) SELECT date_trunc('minute',occurred_at),count(*) FILTER(WHERE event_type='dht.resource_discovered'),coalesce(sum(CASE WHEN event_type='dht.query_summary' AND raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint WHEN event_type='dht.query_received' THEN 1 ELSE 0 END),0),coalesce(sum(CASE WHEN event_type='metadata.fetch_summary' AND raw_event->>'failures' ~ '^[0-9]+$' THEN (raw_event->>'failures')::bigint WHEN event_type IN('dht.error','collector.failed') OR event_type LIKE '%.failed' OR event_type LIKE '%_failed' THEN 1 ELSE 0 END),0),coalesce(sum(CASE WHEN event_type='dht.warning' THEN CASE WHEN raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint ELSE 1 END ELSE 0 END),0) FROM probe_event GROUP BY 1");
    }
  }

  private static void saveState(Connection postgres, String source, long eventRowId) throws Exception {
    try (PreparedStatement statement = postgres.prepareStatement("INSERT INTO migration_state(source,event_rowid,snapshot_at,migrated_at) VALUES(?,?,?,now()) ON CONFLICT(source) DO UPDATE SET event_rowid=excluded.event_rowid,snapshot_at=excluded.snapshot_at,migrated_at=now()")) {
      statement.setString(1, source); statement.setLong(2, eventRowId);
      statement.setObject(3, java.sql.Timestamp.from(Instant.now())); statement.executeUpdate();
    }
  }

  private static Map<String,Object> verify(Connection sqlite, Connection postgres) throws Exception {
    List<Map<String,Object>> rows = new ArrayList<>();
    boolean matches = true;
    for (String table : List.of("content","file_entry","probe_event","discovered_resource","metadata_job")) {
      long left = scalar(sqlite, "SELECT count(*) FROM " + table);
      long right = scalar(postgres, "SELECT count(*) FROM " + table);
      boolean match = left == right;
      matches &= match;
      Map<String,Object> row = new LinkedHashMap<>();
      row.put("table", table); row.put("sqlite", left); row.put("postgres", right); row.put("match", match); rows.add(row);
    }
    Map<String,Object> result = new LinkedHashMap<>(); result.put("match", matches); result.put("verification", rows); return result;
  }

  private static long scalar(Connection connection, String sql) throws Exception {
    try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
      return rows.next() ? rows.getLong(1) : 0;
    }
  }
}
