import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import readline from 'node:readline'
import { DatabaseSync } from 'node:sqlite'
import { compactEvent } from './event-utils.js'

function usage() {
  process.stderr.write(`Usage:\n  node src/catalog.js ingest --input <metadata-events.jsonl> [--db <file>]\n  node src/catalog.js search --query <text> [--db <file>] [--limit <n>]\n  node src/catalog.js stats [--db <file>]\n`)
}

function parseArgs(argv) {
  const [command = 'stats', ...args] = argv
  const options = { db: './var/catalog.db', input: null, query: null, limit: 20 }
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    const value = args[++i]
    if (arg === '--db') options.db = value
    else if (arg === '--input') options.input = value
    else if (arg === '--query') options.query = value
    else if (arg === '--limit') options.limit = positiveInt(value, arg)
    else throw new Error(`unknown option: ${arg}`)
  }
  if (!['ingest', 'search', 'stats'].includes(command)) throw new Error(`unknown command: ${command}`)
  if (command === 'ingest' && !options.input) throw new Error('ingest requires --input')
  if (command === 'search' && !options.query?.trim()) throw new Error('search requires --query')
  return { command, options }
}

function positiveInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`)
  return number
}

function openCatalog(dbFile) {
  const resolved = path.resolve(dbFile)
  fs.mkdirSync(path.dirname(resolved), { recursive: true })
  const db = new DatabaseSync(resolved)
  db.exec('PRAGMA busy_timeout = 10000;')
  if (db.prepare('PRAGMA journal_mode').get().journal_mode !== 'wal') db.exec('PRAGMA journal_mode = WAL;')
  db.exec(`
    PRAGMA synchronous = NORMAL;
    CREATE TABLE IF NOT EXISTS content (
      content_id TEXT PRIMARY KEY,
      info_hash TEXT NOT NULL UNIQUE,
      variant TEXT NOT NULL,
      name TEXT NOT NULL,
      total_size INTEGER NOT NULL,
      file_count INTEGER NOT NULL,
      metadata_size INTEGER NOT NULL,
      metadata_sha256 TEXT NOT NULL,
      policy_state TEXT NOT NULL DEFAULT 'approved',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS file_entry (
      content_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      path TEXT NOT NULL,
      size INTEGER NOT NULL,
      PRIMARY KEY (content_id, ordinal),
      FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS probe_event (
      event_id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      occurred_at TEXT NOT NULL,
      info_hash TEXT,
      peer_host TEXT,
      peer_port INTEGER,
      source_host TEXT,
      source_port INTEGER,
      mode TEXT,
      message TEXT,
      raw_event TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS discovered_resource (
      info_hash TEXT PRIMARY KEY,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      source TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'active'
    );
    CREATE TABLE IF NOT EXISTS metadata_job (
      info_hash TEXT PRIMARY KEY,
      priority INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS probe_event_occurred_at_idx ON probe_event (occurred_at DESC);
    CREATE INDEX IF NOT EXISTS probe_event_type_idx ON probe_event (event_type, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS probe_event_info_hash_idx ON probe_event (info_hash, occurred_at DESC);
    CREATE INDEX IF NOT EXISTS metadata_job_due_idx ON metadata_job (priority DESC, next_attempt_at ASC);
    CREATE VIRTUAL TABLE IF NOT EXISTS content_fts USING fts5(
      content_id UNINDEXED,
      name,
      files,
      tokenize = 'unicode61 remove_diacritics 2'
    );
  `)
  const discoveredColumns = new Set(db.prepare('PRAGMA table_info(discovered_resource)').all().map((column) => column.name))
  if (!discoveredColumns.has('last_seen_at')) db.exec('ALTER TABLE discovered_resource ADD COLUMN last_seen_at TEXT')
  if (!discoveredColumns.has('state')) db.exec("ALTER TABLE discovered_resource ADD COLUMN state TEXT NOT NULL DEFAULT 'active'")
  const needsResourceBackfill = db.prepare(`
    SELECT 1 AS found
    FROM discovered_resource
    WHERE last_seen_at IS NULL OR state IS NULL
    LIMIT 1
  `).get()
  if (needsResourceBackfill) db.exec(`
    UPDATE discovered_resource
    SET last_seen_at = COALESCE(last_seen_at, first_seen_at),
        state = COALESCE(state, 'active')
    WHERE last_seen_at IS NULL OR state IS NULL
  `)
  db.exec(`
    CREATE INDEX IF NOT EXISTS discovered_resource_state_seen_idx
      ON discovered_resource (state, last_seen_at);
  `)
  return db
}

function ingestEvent(db, event) {
  if (!event || !event.event_id || !event.event || !event.occurred_at) return false
  const peer = event.peer || {}
  const source = event.discovered_from || {}
  db.prepare(`
    INSERT OR IGNORE INTO probe_event
      (event_id, event_type, occurred_at, info_hash, peer_host, peer_port,
       source_host, source_port, mode, message, raw_event)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    event.event_id,
    event.event,
    event.occurred_at,
    event.info_hash || null,
    peer.host || null,
    Number.isInteger(peer.port) ? peer.port : null,
    source.host || null,
    Number.isInteger(source.port) ? source.port : null,
    event.mode || null,
    event.message || null,
    JSON.stringify(compactEvent(event))
  )
  if (['metadata.fetch_completed', 'metadata.import_completed'].includes(event.event) && upsertManifest(db, event.manifest)) return true
  return true
}

function upsertManifest(db, manifest) {
  if (!manifest || !/^[a-f0-9]{40}$/.test(manifest.info_hash || '') || !manifest.name) return false
  const now = new Date().toISOString()
  const contentId = `btih:${manifest.info_hash}`
  const files = Array.isArray(manifest.files) ? manifest.files : []
  db.exec('BEGIN')
  try {
    db.prepare(`
      INSERT INTO content
        (content_id, info_hash, variant, name, total_size, file_count,
         metadata_size, metadata_sha256, policy_state, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?)
      ON CONFLICT(content_id) DO UPDATE SET
        variant=excluded.variant,
        name=excluded.name,
        total_size=excluded.total_size,
        file_count=excluded.file_count,
        metadata_size=excluded.metadata_size,
        metadata_sha256=excluded.metadata_sha256,
        updated_at=excluded.updated_at
    `).run(
      contentId,
      manifest.info_hash,
      manifest.variant || 'v1',
      manifest.name,
      manifest.total_size || 0,
      manifest.file_count || files.length,
      manifest.metadata_size || 0,
      manifest.metadata_sha256 || '',
      now,
      now
    )
    db.prepare('DELETE FROM file_entry WHERE content_id = ?').run(contentId)
    const insertFile = db.prepare('INSERT INTO file_entry (content_id, ordinal, path, size) VALUES (?, ?, ?, ?)')
    for (let i = 0; i < files.length; i++) insertFile.run(contentId, i, files[i].path, files[i].size || 0)
    db.prepare('DELETE FROM content_fts WHERE content_id = ?').run(contentId)
    const filesText = files.map((file) => file.path).join(' ')
    db.prepare('INSERT INTO content_fts (content_id, name, files) VALUES (?, ?, ?)').run(contentId, manifest.name, filesText)
    db.exec('COMMIT')
    return true
  } catch (error) {
    db.exec('ROLLBACK')
    throw error
  }
}

function safeMatchQuery(query) {
  return query.trim().split(/\s+/).filter(Boolean).map((term) => `"${term.replaceAll('"', '""')}"`).join(' AND ')
}

function searchCatalogPage(db, query, limit, offset = 0) {
  const match = safeMatchQuery(query)
  let total = db.prepare(`
    SELECT COUNT(*) AS count
    FROM content_fts f
    JOIN content c ON c.content_id = f.content_id
    WHERE content_fts MATCH ? AND c.policy_state = 'approved'
  `).get(match).count
  let rows = total ? db.prepare(`
    SELECT c.content_id, c.info_hash, c.variant, c.name, c.total_size,
           c.file_count, c.updated_at
    FROM content_fts f
    JOIN content c ON c.content_id = f.content_id
    WHERE content_fts MATCH ? AND c.policy_state = 'approved'
    ORDER BY bm25(content_fts), c.updated_at DESC
    LIMIT ? OFFSET ?
  `).all(match, limit, offset) : []

  if (total === 0) {
    const like = `%${query.trim()}%`
    total = db.prepare(`
      SELECT COUNT(*) AS count
      FROM content c
      WHERE c.policy_state = 'approved' AND (c.name LIKE ? OR EXISTS (
        SELECT 1 FROM file_entry f WHERE f.content_id = c.content_id AND f.path LIKE ?
      ))
    `).get(like, like).count
    rows = db.prepare(`
      SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
      FROM content
      WHERE policy_state = 'approved' AND (name LIKE ? OR content_id IN (
        SELECT content_id FROM file_entry WHERE path LIKE ?
      ))
      ORDER BY updated_at DESC
      LIMIT ? OFFSET ?
    `).all(like, like, limit, offset)
  }
  return { results: rows, total }
}

function searchCatalog(db, query, limit, offset = 0) {
  return searchCatalogPage(db, query, limit, offset).results
}

function listCatalogPage(db, limit, offset = 0) {
  const total = db.prepare("SELECT COUNT(*) AS count FROM content WHERE policy_state = 'approved'").get().count
  const results = db.prepare(`
    SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
    FROM content
    WHERE policy_state = 'approved'
    ORDER BY updated_at DESC
    LIMIT ? OFFSET ?
  `).all(limit, offset)
  return { results, total }
}

function dashboardTrend(db, minutes = 5) {
  const bucketMs = 60 * 1000
  const bucketCount = Math.max(1, Math.floor(minutes))
  const currentBucket = Math.floor(Date.now() / bucketMs) * bucketMs
  const startMs = currentBucket - ((bucketCount - 1) * bucketMs)
  const buckets = Array.from({ length: bucketCount }, (_, index) => ({
    at: new Date(startMs + (index * bucketMs)).toISOString(),
    links: 0,
    queries: 0,
    failures: 0,
    warnings: 0
  }))
  const rows = db.prepare(`
    SELECT event_type, occurred_at, raw_event
    FROM probe_event
    WHERE occurred_at >= ?
    ORDER BY occurred_at ASC
  `).all(new Date(startMs).toISOString())
  for (const row of rows) {
    const occurredAt = Date.parse(row.occurred_at)
    const index = Math.floor((occurredAt - startMs) / bucketMs)
    if (index < 0 || index >= buckets.length) continue
    if (row.event_type === 'dht.resource_discovered') buckets[index].links += 1
    else if (row.event_type === 'dht.query_received' || row.event_type === 'dht.query_summary') {
      let occurrences = 1
      if (row.event_type === 'dht.query_summary') {
        try {
          occurrences = Math.max(1, Number(JSON.parse(row.raw_event).occurrences || 1))
        } catch {
          occurrences = 1
        }
      }
      buckets[index].queries += occurrences
    }
    else if (row.event_type === 'dht.warning' || row.event_type === 'metadata.fetch_summary') {
      let occurrences = 1
      try {
        const event = JSON.parse(row.raw_event)
        occurrences = row.event_type === 'dht.warning'
          ? Math.max(1, Number(event.occurrences || 1))
          : Math.max(0, Number(event.failures || 0))
      } catch {
        occurrences = 1
      }
      if (row.event_type === 'dht.warning') buckets[index].warnings += occurrences
      else buckets[index].failures += occurrences
    }
    else if (row.event_type === 'dht.error' || row.event_type === 'collector.failed' || row.event_type.endsWith('.failed') || row.event_type.endsWith('_failed')) buckets[index].failures += 1
  }
  return {
    from: buckets[0].at,
    to: new Date(currentBucket + bucketMs).toISOString(),
    bucket_seconds: 60,
    buckets
  }
}

async function ingest(db, input) {
  const stream = input === '-' ? process.stdin : fs.createReadStream(path.resolve(input), { encoding: 'utf8' })
  const lines = readline.createInterface({ input: stream, crlfDelay: Infinity })
  let accepted = 0
  for await (const line of lines) {
    try {
      const event = JSON.parse(line)
      if (ingestEvent(db, event)) accepted += 1
    } catch (error) {
      process.stderr.write(`catalog skipped event: ${error.message}\n`)
    }
  }
  return accepted
}

async function main() {
  let parsed
  try {
    parsed = parseArgs(process.argv.slice(2))
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    usage()
    process.exitCode = 2
    return
  }

  const db = openCatalog(parsed.options.db)
  try {
    if (parsed.command === 'ingest') {
      const accepted = await ingest(db, parsed.options.input)
      process.stdout.write(`${JSON.stringify({ accepted, db: path.resolve(parsed.options.db) })}\n`)
    } else if (parsed.command === 'search') {
      process.stdout.write(`${JSON.stringify({ query: parsed.options.query, results: searchCatalog(db, parsed.options.query, parsed.options.limit) })}\n`)
    } else {
      process.stdout.write(`${JSON.stringify({ ...dashboardSummary(db), db: path.resolve(parsed.options.db) })}\n`)
    }
  } finally {
    db.close()
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) main()

function dashboardSummary(db) {
  return {
    content: db.prepare('SELECT COUNT(*) AS count FROM content').get().count,
    files: db.prepare('SELECT COUNT(*) AS count FROM file_entry').get().count,
    probes: db.prepare('SELECT COUNT(*) AS count FROM probe_event').get().count,
    peers: db.prepare("SELECT COUNT(*) AS count FROM probe_event WHERE event_type = 'dht.peer_discovered'").get().count,
    lookups: db.prepare("SELECT COUNT(*) AS count FROM probe_event WHERE event_type = 'dht.lookup_completed'").get().count,
    discovered: db.prepare('SELECT COUNT(*) AS count FROM discovered_resource').get().count,
    active_discovered: db.prepare("SELECT COUNT(*) AS count FROM discovered_resource WHERE state = 'active'").get().count,
    invalid_discovered: db.prepare("SELECT COUNT(*) AS count FROM discovered_resource WHERE state = 'invalid'").get().count,
    last_event_at: db.prepare('SELECT MAX(occurred_at) AS value FROM probe_event').get().value
  }
}

function claimDiscoveredResource(db, infoHash, occurredAt, source = 'dht') {
  if (!/^[a-f0-9]{40}$/.test(infoHash || '')) return false
  const result = db.prepare(`
    INSERT OR IGNORE INTO discovered_resource
      (info_hash, first_seen_at, last_seen_at, source, state)
    VALUES (?, ?, ?, ?, 'active')
  `).run(infoHash, occurredAt, occurredAt, source)
  if (!result.changes) db.prepare(`
    UPDATE discovered_resource
    SET last_seen_at = MAX(COALESCE(last_seen_at, first_seen_at), ?),
        state = 'active'
    WHERE info_hash = ?
  `).run(occurredAt, infoHash)
  return result.changes > 0
}

function listRecentResourceObservations(db, cutoff) {
  return db.prepare(`
    SELECT info_hash, last_seen_at
    FROM discovered_resource
    WHERE state = 'active' AND last_seen_at >= ?
  `).all(cutoff).map((row) => ({ info_hash: row.info_hash, last_seen_at: row.last_seen_at }))
}

function countDiscoveredResources(db) {
  return db.prepare('SELECT COUNT(*) AS count FROM discovered_resource').get().count
}

function hasDiscoveredResource(db, infoHash) {
  return Boolean(db.prepare('SELECT 1 AS found FROM discovered_resource WHERE info_hash = ?').get(infoHash))
}

function touchDiscoveredResources(db, observations) {
  if (!observations?.size) return 0
  const update = db.prepare(`
    UPDATE discovered_resource
    SET last_seen_at = MAX(COALESCE(last_seen_at, first_seen_at), ?),
        state = 'active'
    WHERE info_hash = ?
  `)
  let touched = 0
  db.exec('BEGIN IMMEDIATE')
  try {
    for (const [infoHash, occurredAt] of observations) touched += update.run(occurredAt, infoHash).changes
    db.exec('COMMIT')
    return touched
  } catch (error) {
    db.exec('ROLLBACK')
    throw error
  }
}

function queueMetadataJob(db, infoHash, occurredAt, priority = 0, accelerate = false) {
  if (!/^[a-f0-9]{40}$/.test(infoHash || '')) return false
  if (db.prepare('SELECT 1 AS found FROM content WHERE info_hash = ?').get(infoHash)) return false
  return db.prepare(`
    INSERT INTO metadata_job (info_hash, priority, attempts, next_attempt_at, updated_at)
    VALUES (?, ?, 0, ?, ?)
    ON CONFLICT(info_hash) DO UPDATE SET
      priority = MAX(metadata_job.priority, excluded.priority),
      next_attempt_at = CASE
        WHEN ? THEN MIN(metadata_job.next_attempt_at, excluded.next_attempt_at)
        ELSE metadata_job.next_attempt_at
      END,
      updated_at = excluded.updated_at
  `).run(infoHash, priority, occurredAt, occurredAt, accelerate ? 1 : 0).changes > 0
}

function seedMetadataJobs(db, occurredAt = new Date().toISOString()) {
  return db.prepare(`
    INSERT OR IGNORE INTO metadata_job
      (info_hash, priority, attempts, next_attempt_at, updated_at)
    SELECT d.info_hash, 1, 0, ?, d.last_seen_at
    FROM discovered_resource d
    LEFT JOIN content c ON c.info_hash = d.info_hash
    WHERE d.state = 'active' AND c.info_hash IS NULL
  `).run(occurredAt).changes
}

function claimMetadataJob(db, infoHash, occurredAt = new Date().toISOString()) {
  const leaseUntil = new Date(Date.parse(occurredAt) + (10 * 60 * 1000)).toISOString()
  const result = db.prepare(`
    UPDATE metadata_job
    SET attempts = attempts + 1,
        priority = 0,
        next_attempt_at = ?,
        updated_at = ?
    WHERE info_hash = ? AND next_attempt_at <= ?
  `).run(leaseUntil, occurredAt, infoHash, occurredAt)
  return result.changes ? { info_hash: infoHash } : null
}

function claimMetadataJobs(db, limit, occurredAt = new Date().toISOString()) {
  if (!Number.isSafeInteger(limit) || limit <= 0) return []
  db.exec('BEGIN IMMEDIATE')
  try {
    const jobs = db.prepare(`
      SELECT j.info_hash, j.attempts
      FROM metadata_job j
      LEFT JOIN content c ON c.info_hash = j.info_hash
      WHERE j.next_attempt_at <= ? AND c.info_hash IS NULL
      ORDER BY j.priority DESC, j.updated_at DESC
      LIMIT ?
    `).all(occurredAt, limit)
    const update = db.prepare(`
      UPDATE metadata_job
      SET attempts = attempts + 1,
          priority = 0,
          next_attempt_at = ?,
          updated_at = ?
      WHERE info_hash = ?
    `)
    for (const job of jobs) {
      const leaseUntil = new Date(Date.parse(occurredAt) + (10 * 60 * 1000)).toISOString()
      update.run(leaseUntil, occurredAt, job.info_hash)
    }
    db.exec('COMMIT')
    return jobs
  } catch (error) {
    db.exec('ROLLBACK')
    throw error
  }
}

function completeMetadataJob(db, infoHash, succeeded, occurredAt = new Date().toISOString()) {
  if (succeeded) return db.prepare('DELETE FROM metadata_job WHERE info_hash = ?').run(infoHash).changes > 0
  const job = db.prepare('SELECT attempts FROM metadata_job WHERE info_hash = ?').get(infoHash)
  if (!job) return false
  const delayHours = Math.min(24 * 7, 2 ** Math.min(Math.max(job.attempts - 1, 0), 8))
  const retryAt = new Date(Date.parse(occurredAt) + (delayHours * 60 * 60 * 1000)).toISOString()
  return db.prepare(`
    UPDATE metadata_job
    SET next_attempt_at = ?, updated_at = ?
    WHERE info_hash = ?
  `).run(retryAt, occurredAt, infoHash).changes > 0
}

function markInvalidDiscoveredResources(db, cutoff) {
  return db.prepare(`
    UPDATE discovered_resource
    SET state = 'invalid'
    WHERE state != 'invalid' AND last_seen_at < ?
  `).run(cutoff).changes
}

function dashboardData(db, limit = 100) {
  return {
    summary: dashboardSummary(db),
    trend: dashboardTrend(db, 5),
    probes: db.prepare(`
      SELECT event_id, event_type, occurred_at, info_hash, peer_host, peer_port,
             source_host, source_port, mode, message
      FROM probe_event
      ORDER BY occurred_at DESC
      LIMIT ?
    `).all(limit),
    content: db.prepare(`
      SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
      FROM content
      WHERE policy_state = 'approved'
      ORDER BY updated_at DESC
      LIMIT ?
    `).all(limit)
  }
}

export { claimDiscoveredResource, claimMetadataJob, claimMetadataJobs, compactEvent, completeMetadataJob, countDiscoveredResources, dashboardData, dashboardSummary, dashboardTrend, hasDiscoveredResource, ingestEvent, listCatalogPage, listRecentResourceObservations, markInvalidDiscoveredResources, openCatalog, queueMetadataJob, searchCatalog, searchCatalogPage, seedMetadataJobs, touchDiscoveredResources, upsertManifest }
