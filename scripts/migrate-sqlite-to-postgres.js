import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import { once } from 'node:events'
import { DatabaseSync } from 'node:sqlite'
import pg from 'pg'
import { from as copyFrom } from 'pg-copy-streams'
import { connectionOptions, schema } from '../src/postgres-catalog.js'

const { Client } = pg
const LOCK_ID = 374810225

function parseArgs(argv) {
  const options = { db: './var/dht-search.db', mode: 'full' }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    const value = argv[++i]
    if (arg === '--db') options.db = value
    else if (arg === '--mode' && ['full', 'delta', 'verify'].includes(value)) options.mode = value
    else throw new Error(`unknown option: ${arg}${value ? ` ${value}` : ''}`)
  }
  return options
}

function csv(value) {
  if (value === null || value === undefined) return '\\N'
  const text = String(value)
  return `"${text.replaceAll('"', '""')}"`
}

async function copyRows(client, target, columns, rows, label) {
  const stream = client.query(copyFrom(`COPY ${target} (${columns.join(',')}) FROM STDIN WITH (FORMAT csv, NULL '\\N')`))
  let count = 0
  let chunk = ''
  for (const row of rows) {
    chunk += `${columns.map((column) => csv(row[column])).join(',')}\n`
    count += 1
    if (chunk.length >= 1024 * 1024) {
      if (!stream.write(chunk)) await once(stream, 'drain')
      chunk = ''
    }
    if (count % 100_000 === 0) process.stdout.write(`${label}: ${count.toLocaleString()}\n`)
  }
  if (chunk && !stream.write(chunk)) await once(stream, 'drain')
  stream.end()
  await once(stream, 'finish')
  process.stdout.write(`${label}: ${count.toLocaleString()} complete\n`)
  return count
}

async function prepareTarget(client) {
  await client.query(schema)
  await client.query(`
    CREATE TABLE IF NOT EXISTS migration_state (
      source text PRIMARY KEY,
      event_rowid bigint NOT NULL,
      snapshot_at timestamptz NOT NULL,
      migrated_at timestamptz NOT NULL DEFAULT now()
    )
  `)
}

async function rebuildDerivedData(client) {
  process.stdout.write('rebuilding search text, counters, and minute metrics\n')
  await client.query(`
    UPDATE content SET files_text = '';
    UPDATE content c SET files_text = f.files_text
    FROM (SELECT content_id, string_agg(path, ' ' ORDER BY ordinal) AS files_text
          FROM file_entry GROUP BY content_id) f
    WHERE f.content_id = c.content_id;
    TRUNCATE catalog_counter;
    INSERT INTO catalog_counter (name, value) VALUES
      ('content', (SELECT count(*) FROM content)),
      ('files', (SELECT count(*) FROM file_entry)),
      ('probes', (SELECT count(*) FROM probe_event)),
      ('peers', (SELECT count(*) FROM probe_event WHERE event_type='dht.peer_discovered')),
      ('lookups', (SELECT count(*) FROM probe_event WHERE event_type='dht.lookup_completed')),
      ('discovered', (SELECT count(*) FROM discovered_resource));
    TRUNCATE minute_metric;
    INSERT INTO minute_metric (bucket, links, queries, failures, warnings)
    SELECT date_trunc('minute', occurred_at),
      count(*) FILTER (WHERE event_type='dht.resource_discovered'),
      coalesce(sum(CASE
        WHEN event_type='dht.query_summary' THEN CASE WHEN raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint ELSE 1 END
        WHEN event_type='dht.query_received' THEN 1 ELSE 0 END), 0),
      coalesce(sum(CASE
        WHEN event_type='metadata.fetch_summary' THEN CASE WHEN raw_event->>'failures' ~ '^[0-9]+$' THEN (raw_event->>'failures')::bigint ELSE 0 END
        WHEN event_type IN ('dht.error','collector.failed') OR event_type LIKE '%.failed' OR event_type LIKE '%_failed' THEN 1
        ELSE 0 END), 0),
      coalesce(sum(CASE WHEN event_type='dht.warning'
        THEN CASE WHEN raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint ELSE 1 END
        ELSE 0 END), 0)
    FROM probe_event GROUP BY 1;
    CREATE INDEX IF NOT EXISTS content_updated_idx ON content (updated_at DESC);
    CREATE INDEX IF NOT EXISTS probe_event_occurred_at_idx ON probe_event (occurred_at DESC);
    CREATE INDEX IF NOT EXISTS probe_event_info_hash_idx ON probe_event (info_hash, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS discovered_resource_state_seen_idx
      ON discovered_resource (state, last_seen_at);
    CREATE INDEX IF NOT EXISTS metadata_job_due_idx
      ON metadata_job (priority DESC, next_attempt_at ASC, updated_at DESC);
    ANALYZE;
  `)
}

async function applyDeltaDerivedData(client) {
  await client.query(`
    INSERT INTO minute_metric (bucket, links, queries, failures, warnings)
    SELECT date_trunc('minute', occurred_at),
      count(*) FILTER (WHERE event_type='dht.resource_discovered'),
      coalesce(sum(CASE
        WHEN event_type='dht.query_summary' THEN CASE WHEN raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint ELSE 1 END
        WHEN event_type='dht.query_received' THEN 1 ELSE 0 END), 0),
      coalesce(sum(CASE
        WHEN event_type='metadata.fetch_summary' THEN CASE WHEN raw_event->>'failures' ~ '^[0-9]+$' THEN (raw_event->>'failures')::bigint ELSE 0 END
        WHEN event_type IN ('dht.error','collector.failed') OR event_type LIKE '%.failed' OR event_type LIKE '%_failed' THEN 1
        ELSE 0 END), 0),
      coalesce(sum(CASE WHEN event_type='dht.warning'
        THEN CASE WHEN raw_event->>'occurrences' ~ '^[0-9]+$' THEN (raw_event->>'occurrences')::bigint ELSE 1 END
        ELSE 0 END), 0)
    FROM stage_probe_event GROUP BY 1
    ON CONFLICT (bucket) DO UPDATE SET
      links = minute_metric.links + excluded.links,
      queries = minute_metric.queries + excluded.queries,
      failures = minute_metric.failures + excluded.failures,
      warnings = minute_metric.warnings + excluded.warnings;

    INSERT INTO catalog_counter (name, value)
    SELECT name, value FROM (VALUES
      ('probes', (SELECT count(*) FROM stage_probe_event)),
      ('peers', (SELECT count(*) FROM stage_probe_event WHERE event_type='dht.peer_discovered')),
      ('lookups', (SELECT count(*) FROM stage_probe_event WHERE event_type='dht.lookup_completed'))
    ) AS increments(name, value)
    ON CONFLICT (name) DO UPDATE SET value = catalog_counter.value + excluded.value;

    INSERT INTO catalog_counter (name, value)
    SELECT name, value FROM (VALUES
      ('content', (SELECT count(*) FROM content)),
      ('files', (SELECT count(*) FROM file_entry)),
      ('discovered', (SELECT count(*) FROM discovered_resource))
    ) AS totals(name, value)
    ON CONFLICT (name) DO UPDATE SET value = excluded.value;
  `)
}

function rows(db, sql, ...args) {
  return db.prepare(sql).iterate(...args)
}

async function fullMigration(db, client, source) {
  process.stdout.write('starting consistent SQLite snapshot\n')
  const snapshotAt = new Date().toISOString()
  db.exec('BEGIN')
  try {
    const eventRowid = db.prepare('SELECT coalesce(max(rowid), 0) AS value FROM probe_event').get().value
    await client.query(`
      DROP INDEX IF EXISTS content_updated_idx;
      DROP INDEX IF EXISTS probe_event_occurred_at_idx;
      DROP INDEX IF EXISTS probe_event_info_hash_idx;
      DROP INDEX IF EXISTS discovered_resource_state_seen_idx;
      DROP INDEX IF EXISTS metadata_job_due_idx;
      TRUNCATE minute_metric, catalog_counter, metadata_job, file_entry, content,
        discovered_resource, probe_event;
    `)
    await copyRows(client, 'content', [
      'content_id','info_hash','variant','name','total_size','file_count','metadata_size',
      'metadata_sha256','policy_state','files_text','created_at','updated_at'
    ], rows(db, `SELECT content_id,info_hash,variant,name,total_size,file_count,metadata_size,
      metadata_sha256,policy_state,'' AS files_text,created_at,updated_at FROM content`), 'content')
    await copyRows(client, 'file_entry', ['content_id','ordinal','path','size'],
      rows(db, 'SELECT content_id,ordinal,path,size FROM file_entry'), 'file_entry')
    await copyRows(client, 'probe_event', [
      'event_id','event_type','occurred_at','info_hash','peer_host','peer_port',
      'source_host','source_port','mode','message','raw_event'
    ], rows(db, `SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,
      source_host,source_port,mode,message,raw_event FROM probe_event WHERE rowid <= ? ORDER BY rowid`, eventRowid), 'probe_event')
    await copyRows(client, 'discovered_resource', ['info_hash','first_seen_at','last_seen_at','source','state'],
      rows(db, 'SELECT info_hash,first_seen_at,last_seen_at,source,state FROM discovered_resource'), 'discovered_resource')
    await copyRows(client, 'metadata_job', ['info_hash','priority','attempts','next_attempt_at','updated_at'],
      rows(db, 'SELECT info_hash,priority,attempts,next_attempt_at,updated_at FROM metadata_job'), 'metadata_job')
    await client.query(`
      INSERT INTO migration_state (source, event_rowid, snapshot_at, migrated_at)
      VALUES ($1,$2,$3,now())
      ON CONFLICT (source) DO UPDATE SET event_rowid=excluded.event_rowid,
        snapshot_at=excluded.snapshot_at,migrated_at=now()
    `, [source, eventRowid, snapshotAt])
    db.exec('COMMIT')
    await rebuildDerivedData(client)
  } catch (error) {
    db.exec('ROLLBACK')
    throw error
  }
}

async function createDeltaTables(client) {
  await client.query(`
    CREATE TEMP TABLE stage_probe_event (LIKE probe_event INCLUDING DEFAULTS);
    CREATE TEMP TABLE stage_content (LIKE content INCLUDING DEFAULTS);
    CREATE TEMP TABLE stage_file_entry (LIKE file_entry INCLUDING DEFAULTS);
    CREATE TEMP TABLE stage_discovered_resource (LIKE discovered_resource INCLUDING DEFAULTS);
    CREATE TEMP TABLE stage_metadata_job (LIKE metadata_job INCLUDING DEFAULTS);
  `)
}

async function deltaMigration(db, client, source) {
  const state = (await client.query('SELECT * FROM migration_state WHERE source=$1', [source])).rows[0]
  if (!state) throw new Error('no full migration state found; run --mode full first')
  const snapshotAt = new Date().toISOString()
  const contentOverlapAt = new Date(state.snapshot_at.getTime() - 60_000).toISOString()
  db.exec('BEGIN')
  try {
    const eventRowid = db.prepare('SELECT coalesce(max(rowid), 0) AS value FROM probe_event').get().value
    await createDeltaTables(client)
    await copyRows(client, 'stage_probe_event', [
      'event_id','event_type','occurred_at','info_hash','peer_host','peer_port',
      'source_host','source_port','mode','message','raw_event'
    ], rows(db, `SELECT event_id,event_type,occurred_at,info_hash,peer_host,peer_port,
      source_host,source_port,mode,message,raw_event FROM probe_event WHERE rowid > ? AND rowid <= ? ORDER BY rowid`,
      Number(state.event_rowid), eventRowid), 'delta probe_event')
    await copyRows(client, 'stage_content', [
      'content_id','info_hash','variant','name','total_size','file_count','metadata_size',
      'metadata_sha256','policy_state','files_text','created_at','updated_at'
    ], rows(db, `SELECT c.content_id,c.info_hash,c.variant,c.name,c.total_size,c.file_count,c.metadata_size,
      c.metadata_sha256,c.policy_state,
      coalesce((SELECT group_concat(path, ' ') FROM (
        SELECT path FROM file_entry WHERE content_id=c.content_id ORDER BY ordinal
      )), '') AS files_text,
      c.created_at,c.updated_at FROM content c WHERE c.updated_at >= ?`,
      contentOverlapAt), 'delta content')
    await copyRows(client, 'stage_file_entry', ['content_id','ordinal','path','size'],
      rows(db, `SELECT f.content_id,f.ordinal,f.path,f.size FROM file_entry f
        JOIN content c ON c.content_id=f.content_id WHERE c.updated_at >= ?`, contentOverlapAt), 'delta file_entry')
    await copyRows(client, 'stage_discovered_resource', ['info_hash','first_seen_at','last_seen_at','source','state'],
      rows(db, 'SELECT info_hash,first_seen_at,last_seen_at,source,state FROM discovered_resource ORDER BY info_hash'), 'delta discovered_resource')
    await copyRows(client, 'stage_metadata_job', ['info_hash','priority','attempts','next_attempt_at','updated_at'],
      rows(db, 'SELECT info_hash,priority,attempts,next_attempt_at,updated_at FROM metadata_job ORDER BY info_hash'), 'delta metadata_job')
    db.exec('COMMIT')

    await client.query('BEGIN')
    try {
      await client.query(`
        INSERT INTO probe_event SELECT * FROM stage_probe_event;
        INSERT INTO content SELECT * FROM stage_content
          ON CONFLICT (content_id) DO UPDATE SET
            info_hash=excluded.info_hash,variant=excluded.variant,name=excluded.name,
            total_size=excluded.total_size,file_count=excluded.file_count,
            metadata_size=excluded.metadata_size,metadata_sha256=excluded.metadata_sha256,
            policy_state=excluded.policy_state,files_text=excluded.files_text,
            updated_at=excluded.updated_at;
        DELETE FROM file_entry WHERE content_id IN (SELECT content_id FROM stage_content);
        INSERT INTO file_entry SELECT * FROM stage_file_entry;
        TRUNCATE discovered_resource;
        INSERT INTO discovered_resource SELECT * FROM stage_discovered_resource;
        TRUNCATE metadata_job;
        INSERT INTO metadata_job SELECT * FROM stage_metadata_job;
      `)
      await applyDeltaDerivedData(client)
      await client.query(`
        INSERT INTO migration_state (source,event_rowid,snapshot_at,migrated_at)
        VALUES ($1,$2,$3,now()) ON CONFLICT (source) DO UPDATE SET
          event_rowid=excluded.event_rowid,snapshot_at=excluded.snapshot_at,migrated_at=now()
      `, [source, eventRowid, snapshotAt])
      await client.query('COMMIT')
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    }
  } catch (error) {
    try { db.exec('ROLLBACK') } catch {}
    throw error
  }
}

async function counts(db, client) {
  const tables = ['content','file_entry','probe_event','discovered_resource','metadata_job']
  const result = []
  for (const table of tables) {
    const sqlite = Number(db.prepare(`SELECT count(*) AS count FROM ${table}`).get().count)
    const postgres = Number((await client.query(`SELECT count(*)::bigint AS count FROM ${table}`)).rows[0].count)
    result.push({ table, sqlite, postgres, match: sqlite === postgres })
  }
  return result
}

async function reconcileMissingContent(db, client) {
  const remote = new Set((await client.query('SELECT info_hash FROM content')).rows.map((row) => row.info_hash))
  const missing = db.prepare('SELECT * FROM content').all().filter((row) => !remote.has(row.info_hash))
  if (!missing.length) return 0
  const fileQuery = db.prepare('SELECT path, size FROM file_entry WHERE content_id = ? ORDER BY ordinal')
  await client.query('BEGIN')
  try {
    for (const row of missing) {
      const files = fileQuery.all(row.content_id)
      await client.query(`
        INSERT INTO content
          (content_id,info_hash,variant,name,total_size,file_count,metadata_size,
           metadata_sha256,policy_state,files_text,created_at,updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (content_id) DO NOTHING
      `, [row.content_id,row.info_hash,row.variant,row.name,row.total_size,row.file_count,
        row.metadata_size,row.metadata_sha256,row.policy_state,
        files.map((file) => file.path).join(' '),row.created_at,row.updated_at])
      if (files.length) await client.query(`
        INSERT INTO file_entry (content_id,ordinal,path,size)
        SELECT $1, ordinal - 1, path, size
        FROM unnest($2::text[],$3::bigint[]) WITH ORDINALITY AS f(path,size,ordinal)
        ON CONFLICT (content_id,ordinal) DO NOTHING
      `, [row.content_id, files.map((file) => file.path), files.map((file) => file.size)])
    }
    await client.query(`
      INSERT INTO catalog_counter (name,value) VALUES
        ('content',(SELECT count(*) FROM content)),
        ('files',(SELECT count(*) FROM file_entry))
      ON CONFLICT (name) DO UPDATE SET value=excluded.value
    `)
    await client.query('COMMIT')
    process.stdout.write(`content reconciliation: ${missing.length.toLocaleString()} restored\n`)
    return missing.length
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  const source = path.resolve(options.db)
  if (!fs.existsSync(source)) throw new Error(`SQLite database not found: ${source}`)
  const db = new DatabaseSync(source, { readOnly: true })
  db.exec('PRAGMA busy_timeout=30000')
  const client = new Client(connectionOptions())
  await client.connect()
  try {
    await client.query('SELECT pg_advisory_lock($1)', [LOCK_ID])
    await prepareTarget(client)
    if (options.mode === 'full') await fullMigration(db, client, source)
    else if (options.mode === 'delta') await deltaMigration(db, client, source)
    if (options.mode !== 'verify') await reconcileMissingContent(db, client)
    const verification = await counts(db, client)
    process.stdout.write(`${JSON.stringify({ mode: options.mode, verification }, null, 2)}\n`)
    if (options.mode !== 'full' && verification.some((item) => !item.match)) process.exitCode = 3
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [LOCK_ID]).catch(() => {})
    await client.end()
    db.close()
  }
}

main().catch((error) => {
  process.stderr.write(`migration failed: ${error.stack || error.message}\n`)
  process.exitCode = 1
})
