import fs from 'node:fs'
import tls from 'node:tls'
import { Pool } from 'pg'
import { compactEvent } from './event-utils.js'

const EVENT_BATCH_SIZE = 250
const EVENT_FLUSH_MS = 25

function connectionOptions(connectionString = process.env.DATABASE_URL) {
  let expectedHost = process.env.PGHOST
  const options = {}
  if (connectionString) {
    const parsed = new URL(connectionString)
    expectedHost = parsed.hostname
    options.host = parsed.hostname
    if (parsed.port) options.port = Number(parsed.port)
    if (parsed.username) options.user = decodeURIComponent(parsed.username)
    if (parsed.password) options.password = decodeURIComponent(parsed.password)
    if (parsed.pathname.length > 1) options.database = decodeURIComponent(parsed.pathname.slice(1))
  }
  if (process.env.PGSSLMODE && process.env.PGSSLMODE !== 'disable') {
    options.ssl = {
      rejectUnauthorized: !['allow', 'prefer', 'require'].includes(process.env.PGSSLMODE),
      ...(process.env.PGSSLROOTCERT ? { ca: fs.readFileSync(process.env.PGSSLROOTCERT, 'utf8') } : {}),
      ...(process.env.PGSSLMODE === 'verify-full' && expectedHost
        ? { checkServerIdentity: (_hostname, certificate) => tls.checkServerIdentity(expectedHost, certificate) }
        : {})
    }
  }
  return options
}

const schema = `
  CREATE TABLE IF NOT EXISTS content (
    content_id text PRIMARY KEY,
    info_hash text NOT NULL UNIQUE,
    variant text NOT NULL,
    name text NOT NULL,
    total_size bigint NOT NULL,
    file_count integer NOT NULL,
    metadata_size integer NOT NULL,
    metadata_sha256 text NOT NULL,
    policy_state text NOT NULL DEFAULT 'approved',
    files_text text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
  );
  CREATE INDEX IF NOT EXISTS content_updated_idx ON content (updated_at DESC);

  CREATE TABLE IF NOT EXISTS file_entry (
    content_id text NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    path text NOT NULL,
    size bigint NOT NULL,
    PRIMARY KEY (content_id, ordinal)
  );

  CREATE TABLE IF NOT EXISTS probe_event (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    info_hash text,
    peer_host text,
    peer_port integer,
    source_host text,
    source_port integer,
    mode text,
    message text,
    raw_event jsonb NOT NULL
  );
  CREATE INDEX IF NOT EXISTS probe_event_occurred_at_idx ON probe_event (occurred_at DESC);
  CREATE INDEX IF NOT EXISTS probe_event_info_hash_idx ON probe_event (info_hash, occurred_at DESC);

  CREATE TABLE IF NOT EXISTS discovered_resource (
    info_hash text PRIMARY KEY,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    source text NOT NULL,
    state text NOT NULL DEFAULT 'active'
  );
  CREATE INDEX IF NOT EXISTS discovered_resource_state_seen_idx
    ON discovered_resource (state, last_seen_at);

  CREATE TABLE IF NOT EXISTS metadata_job (
    info_hash text PRIMARY KEY,
    priority integer NOT NULL DEFAULT 0,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
  );
  CREATE INDEX IF NOT EXISTS metadata_job_due_idx
    ON metadata_job (priority DESC, next_attempt_at ASC, updated_at DESC);

  CREATE TABLE IF NOT EXISTS catalog_counter (
    name text PRIMARY KEY,
    value bigint NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS minute_metric (
    bucket timestamptz PRIMARY KEY,
    links bigint NOT NULL DEFAULT 0,
    queries bigint NOT NULL DEFAULT 0,
    failures bigint NOT NULL DEFAULT 0,
    warnings bigint NOT NULL DEFAULT 0
  );
`

class PostgresCatalog {
  constructor(pool) {
    this.pool = pool
    this.eventQueue = []
    this.eventTimer = null
    this.flushing = null
    this.closed = false
  }

  async initialize() {
    await this.pool.query(schema)
    return this
  }

  async countDiscoveredResources() {
    const { rows } = await this.pool.query('SELECT count(*)::bigint AS count FROM discovered_resource')
    return Number(rows[0].count)
  }

  async listRecentResourceObservations(cutoff) {
    const { rows } = await this.pool.query(`
      SELECT info_hash, last_seen_at
      FROM discovered_resource
      WHERE state = 'active' AND last_seen_at >= $1
    `, [cutoff])
    return rows.map((row) => ({ info_hash: row.info_hash, last_seen_at: row.last_seen_at.toISOString() }))
  }

  async hasDiscoveredResource(infoHash) {
    return (await this.pool.query('SELECT 1 FROM discovered_resource WHERE info_hash = $1', [infoHash])).rowCount > 0
  }

  ingestEvent(event) {
    if (!event?.event_id || !event?.event || !event?.occurred_at) return Promise.resolve(false)
    if (this.closed) return Promise.reject(new Error('PostgreSQL catalog is closed'))
    return new Promise((resolve, reject) => {
      this.eventQueue.push({ event, resolve, reject })
      if (this.eventQueue.length >= EVENT_BATCH_SIZE) void this.flushEvents()
      else if (!this.eventTimer) {
        this.eventTimer = setTimeout(() => void this.flushEvents(), EVENT_FLUSH_MS)
        this.eventTimer.unref()
      }
    })
  }

  async flushEvents() {
    if (this.eventTimer) clearTimeout(this.eventTimer)
    this.eventTimer = null
    if (this.flushing) {
      await this.flushing
      if (this.eventQueue.length) return this.flushEvents()
      return
    }
    if (!this.eventQueue.length) return
    const batch = this.eventQueue.splice(0, EVENT_BATCH_SIZE)
    this.flushing = this.#writeEventBatch(batch)
    try {
      await this.flushing
      for (const item of batch) item.resolve(true)
    } catch (error) {
      for (const item of batch) item.reject(error)
    } finally {
      this.flushing = null
    }
    if (this.eventQueue.length) return this.flushEvents()
  }

  async #writeEventBatch(batch) {
    const values = batch.map(({ event }) => {
      const peer = event.peer || {}
      const source = event.discovered_from || {}
      return [
        event.event_id, event.event, event.occurred_at, event.info_hash || null,
        peer.host || null, Number.isInteger(peer.port) ? peer.port : null,
        source.host || null, Number.isInteger(source.port) ? source.port : null,
        event.mode || null, event.message || null, JSON.stringify(compactEvent(event))
      ]
    })
    const columns = values[0].map((_, index) => values.map((row) => row[index]))
    await this.pool.query(`
      WITH input AS (
        SELECT * FROM unnest(
          $1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::text[],
          $6::integer[], $7::text[], $8::integer[], $9::text[], $10::text[], $11::jsonb[]
        ) AS t(event_id, event_type, occurred_at, info_hash, peer_host, peer_port,
               source_host, source_port, mode, message, raw_event)
      ), inserted AS MATERIALIZED (
        INSERT INTO probe_event
          (event_id, event_type, occurred_at, info_hash, peer_host, peer_port,
           source_host, source_port, mode, message, raw_event)
        SELECT * FROM input
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_type, occurred_at, raw_event
      ), metric_values AS (
        SELECT date_trunc('minute', occurred_at) AS bucket,
          count(*) FILTER (WHERE event_type = 'dht.resource_discovered') AS links,
          coalesce(sum(CASE
            WHEN event_type = 'dht.query_summary' THEN coalesce((raw_event->>'occurrences')::bigint, 1)
            WHEN event_type = 'dht.query_received' THEN 1 ELSE 0 END), 0) AS queries,
          coalesce(sum(CASE
            WHEN event_type = 'metadata.fetch_summary' THEN coalesce((raw_event->>'failures')::bigint, 0)
            WHEN event_type IN ('dht.error', 'collector.failed') OR event_type LIKE '%.failed' OR event_type LIKE '%_failed' THEN 1
            ELSE 0 END), 0) AS failures,
          coalesce(sum(CASE WHEN event_type = 'dht.warning'
            THEN coalesce((raw_event->>'occurrences')::bigint, 1) ELSE 0 END), 0) AS warnings
        FROM inserted GROUP BY 1
      ), metric_upsert AS (
        INSERT INTO minute_metric (bucket, links, queries, failures, warnings)
        SELECT bucket, links, queries, failures, warnings FROM metric_values
        ON CONFLICT (bucket) DO UPDATE SET
          links = minute_metric.links + excluded.links,
          queries = minute_metric.queries + excluded.queries,
          failures = minute_metric.failures + excluded.failures,
          warnings = minute_metric.warnings + excluded.warnings
      ), counter_values AS (
        SELECT * FROM (VALUES
          ('probes', (SELECT count(*) FROM inserted)),
          ('peers', (SELECT count(*) FROM inserted WHERE event_type = 'dht.peer_discovered')),
          ('lookups', (SELECT count(*) FROM inserted WHERE event_type = 'dht.lookup_completed'))
        ) AS counters(name, value)
      )
      INSERT INTO catalog_counter (name, value)
      SELECT name, value FROM counter_values
      ON CONFLICT (name) DO UPDATE SET value = catalog_counter.value + excluded.value
    `, columns)
    const manifests = batch.map(({ event }) => event)
      .filter((event) => ['metadata.fetch_completed', 'metadata.import_completed'].includes(event.event) && event.manifest)
    for (const event of manifests) await this.upsertManifest(event.manifest)
  }

  async upsertManifest(manifest) {
    if (!manifest || !/^[a-f0-9]{40}$/.test(manifest.info_hash || '') || !manifest.name) return false
    const now = new Date().toISOString()
    const contentId = `btih:${manifest.info_hash}`
    const files = Array.isArray(manifest.files) ? manifest.files : []
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      await client.query(`
        WITH upsert AS (
        INSERT INTO content
          (content_id, info_hash, variant, name, total_size, file_count, metadata_size,
           metadata_sha256, policy_state, files_text, created_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'approved',$9,$10,$10)
        ON CONFLICT (content_id) DO UPDATE SET
          variant=excluded.variant, name=excluded.name, total_size=excluded.total_size,
          file_count=excluded.file_count, metadata_size=excluded.metadata_size,
          metadata_sha256=excluded.metadata_sha256, files_text=excluded.files_text,
          updated_at=excluded.updated_at
        RETURNING (xmax = 0) AS inserted
        )
        INSERT INTO catalog_counter (name, value)
        SELECT 'content', count(*) FROM upsert WHERE inserted
        ON CONFLICT (name) DO UPDATE SET value = catalog_counter.value + excluded.value
      `, [contentId, manifest.info_hash, manifest.variant || 'v1', manifest.name,
        manifest.total_size || 0, manifest.file_count || files.length,
        manifest.metadata_size || 0, manifest.metadata_sha256 || '',
        files.map((file) => file.path).join(' '), now])
      const previousFiles = await client.query('DELETE FROM file_entry WHERE content_id = $1 RETURNING 1', [contentId])
      if (files.length) {
        await client.query(`
          INSERT INTO file_entry (content_id, ordinal, path, size)
          SELECT $1, ordinal - 1, path, size
          FROM unnest($2::text[], $3::bigint[]) WITH ORDINALITY AS f(path, size, ordinal)
        `, [contentId, files.map((file) => file.path), files.map((file) => file.size || 0)])
      }
      await client.query(`
        INSERT INTO catalog_counter (name, value) VALUES ('files', $1)
        ON CONFLICT (name) DO UPDATE SET value = catalog_counter.value + excluded.value
      `, [files.length - previousFiles.rowCount])
      await client.query('COMMIT')
      return true
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    } finally {
      client.release()
    }
  }

  async claimDiscoveredResource(infoHash, occurredAt, source = 'dht') {
    if (!/^[a-f0-9]{40}$/.test(infoHash || '')) return false
    const { rows } = await this.pool.query(`
      WITH upsert AS (
      INSERT INTO discovered_resource (info_hash, first_seen_at, last_seen_at, source, state)
      VALUES ($1, $2, $2, $3, 'active')
      ON CONFLICT (info_hash) DO UPDATE SET
        last_seen_at = greatest(discovered_resource.last_seen_at, excluded.last_seen_at),
        state = 'active'
      RETURNING (xmax = 0) AS newly_discovered
      ), counter AS (
        INSERT INTO catalog_counter (name, value)
        SELECT 'discovered', count(*) FROM upsert WHERE newly_discovered
        ON CONFLICT (name) DO UPDATE SET value = catalog_counter.value + excluded.value
      )
      SELECT newly_discovered FROM upsert
    `, [infoHash, occurredAt, source])
    return rows[0]?.newly_discovered === true
  }

  async touchDiscoveredResources(observations) {
    if (!observations?.size) return 0
    const hashes = [...observations.keys()]
    const times = [...observations.values()]
    const { rowCount } = await this.pool.query(`
      UPDATE discovered_resource d SET
        last_seen_at = greatest(d.last_seen_at, observed.occurred_at), state = 'active'
      FROM unnest($1::text[], $2::timestamptz[]) AS observed(info_hash, occurred_at)
      WHERE d.info_hash = observed.info_hash
    `, [hashes, times])
    return rowCount
  }

  async queueMetadataJob(infoHash, occurredAt, priority = 0, accelerate = false) {
    if (!/^[a-f0-9]{40}$/.test(infoHash || '')) return false
    const { rowCount } = await this.pool.query(`
      INSERT INTO metadata_job (info_hash, priority, attempts, next_attempt_at, updated_at)
      SELECT $1, $3, 0, $2, $2
      WHERE NOT EXISTS (SELECT 1 FROM content WHERE info_hash = $1)
      ON CONFLICT (info_hash) DO UPDATE SET
        priority = greatest(metadata_job.priority, excluded.priority),
        next_attempt_at = CASE WHEN $4 THEN least(metadata_job.next_attempt_at, excluded.next_attempt_at)
          ELSE metadata_job.next_attempt_at END,
        updated_at = excluded.updated_at
    `, [infoHash, occurredAt, priority, accelerate])
    return rowCount > 0
  }

  async seedMetadataJobs(occurredAt = new Date().toISOString()) {
    if ((await this.pool.query('SELECT 1 FROM metadata_job LIMIT 1')).rowCount) return 0
    const { rowCount } = await this.pool.query(`
      INSERT INTO metadata_job (info_hash, priority, attempts, next_attempt_at, updated_at)
      SELECT d.info_hash, 1, 0, $1, d.last_seen_at
      FROM discovered_resource d
      WHERE d.state = 'active' AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash = d.info_hash)
      ON CONFLICT (info_hash) DO NOTHING
    `, [occurredAt])
    return rowCount
  }

  async claimMetadataJob(infoHash, occurredAt = new Date().toISOString()) {
    const { rows } = await this.pool.query(`
      UPDATE metadata_job SET attempts = attempts + 1, priority = 0,
        next_attempt_at = $2::timestamptz + interval '10 minutes', updated_at = $2
      WHERE info_hash = $1 AND next_attempt_at <= $2
      RETURNING info_hash
    `, [infoHash, occurredAt])
    return rows[0] || null
  }

  async claimMetadataJobs(limit, occurredAt = new Date().toISOString()) {
    if (!Number.isSafeInteger(limit) || limit <= 0) return []
    const { rows } = await this.pool.query(`
      WITH due AS (
        SELECT j.info_hash, j.attempts
        FROM metadata_job j
        WHERE j.next_attempt_at <= $1
          AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash = j.info_hash)
        ORDER BY j.priority DESC, j.next_attempt_at ASC, j.updated_at DESC
        FOR UPDATE OF j SKIP LOCKED
        LIMIT $2
      )
      UPDATE metadata_job j SET attempts = j.attempts + 1, priority = 0,
        next_attempt_at = $1::timestamptz + interval '10 minutes', updated_at = $1
      FROM due WHERE j.info_hash = due.info_hash
      RETURNING j.info_hash, due.attempts
    `, [occurredAt, limit])
    return rows
  }

  async completeMetadataJob(infoHash, succeeded, occurredAt = new Date().toISOString()) {
    if (succeeded) return (await this.pool.query('DELETE FROM metadata_job WHERE info_hash = $1', [infoHash])).rowCount > 0
    const { rowCount } = await this.pool.query(`
      UPDATE metadata_job SET
        next_attempt_at = $2::timestamptz + make_interval(hours => least(168, power(2, least(greatest(attempts - 1, 0), 8))::integer)),
        updated_at = $2
      WHERE info_hash = $1
    `, [infoHash, occurredAt])
    return rowCount > 0
  }

  async markInvalidDiscoveredResources(cutoff) {
    return (await this.pool.query(`
      UPDATE discovered_resource SET state = 'invalid'
      WHERE state != 'invalid' AND last_seen_at < $1
    `, [cutoff])).rowCount
  }

  async dashboardSummary() {
    const { rows } = await this.pool.query(`
      SELECT
        coalesce((SELECT value FROM catalog_counter WHERE name='content'), 0)::bigint AS content,
        coalesce((SELECT value FROM catalog_counter WHERE name='files'), 0)::bigint AS files,
        coalesce((SELECT value FROM catalog_counter WHERE name='probes'), 0)::bigint AS probes,
        coalesce((SELECT value FROM catalog_counter WHERE name='peers'), 0)::bigint AS peers,
        coalesce((SELECT value FROM catalog_counter WHERE name='lookups'), 0)::bigint AS lookups,
        coalesce((SELECT value FROM catalog_counter WHERE name='discovered'), 0)::bigint AS discovered,
        coalesce((SELECT count(*) FROM discovered_resource WHERE state='active'), 0)::bigint AS active_discovered,
        coalesce((SELECT count(*) FROM discovered_resource WHERE state='invalid'), 0)::bigint AS invalid_discovered,
        (SELECT occurred_at FROM probe_event ORDER BY occurred_at DESC LIMIT 1) AS last_event_at
    `)
    const row = rows[0]
    for (const key of ['content', 'files', 'probes', 'peers', 'lookups', 'discovered', 'active_discovered', 'invalid_discovered']) row[key] = Number(row[key])
    if (row.last_event_at) row.last_event_at = row.last_event_at.toISOString()
    return row
  }

  async dashboardTrend(minutes = 5) {
    const bucketCount = Math.max(1, Math.floor(minutes))
    const { rows } = await this.pool.query(`
      WITH bounds AS (
        SELECT date_trunc('minute', now()) AS current_bucket
      ), buckets AS (
        SELECT generate_series(current_bucket - ($1::integer - 1) * interval '1 minute',
          current_bucket, interval '1 minute') AS bucket FROM bounds
      )
      SELECT b.bucket, coalesce(m.links,0)::bigint AS links, coalesce(m.queries,0)::bigint AS queries,
        coalesce(m.failures,0)::bigint AS failures, coalesce(m.warnings,0)::bigint AS warnings
      FROM buckets b LEFT JOIN minute_metric m USING (bucket) ORDER BY b.bucket
    `, [bucketCount])
    const buckets = rows.map((row) => ({
      at: row.bucket.toISOString(), links: Number(row.links), queries: Number(row.queries),
      failures: Number(row.failures), warnings: Number(row.warnings)
    }))
    return {
      from: buckets[0].at,
      to: new Date(new Date(buckets.at(-1).at).getTime() + 60_000).toISOString(),
      bucket_seconds: 60,
      buckets
    }
  }

  async dashboardData(limit = 100) {
    const [summary, trend, probes, content] = await Promise.all([
      this.dashboardSummary(), this.dashboardTrend(5),
      this.pool.query(`SELECT event_id, event_type, occurred_at, info_hash, peer_host, peer_port,
        source_host, source_port, mode, message FROM probe_event ORDER BY occurred_at DESC LIMIT $1`, [limit]),
      this.pool.query(`SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
        FROM content WHERE policy_state='approved' ORDER BY updated_at DESC LIMIT $1`, [limit])
    ])
    const normalize = (rows) => rows.map((row) => ({
      ...row,
      ...(row.occurred_at ? { occurred_at: row.occurred_at.toISOString() } : {}),
      ...(row.updated_at ? { updated_at: row.updated_at.toISOString() } : {}),
      ...(row.total_size != null ? { total_size: Number(row.total_size) } : {})
    }))
    return { summary, trend, probes: normalize(probes.rows), content: normalize(content.rows) }
  }

  async listCatalogPage(limit, offset = 0) {
    const [count, result] = await Promise.all([
      this.pool.query("SELECT count(*)::bigint AS count FROM content WHERE policy_state='approved'"),
      this.pool.query(`SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
        FROM content WHERE policy_state='approved' ORDER BY updated_at DESC LIMIT $1 OFFSET $2`, [limit, offset])
    ])
    return { results: normalizeContent(result.rows), total: Number(count.rows[0].count) }
  }

  async searchCatalogPage(query, limit, offset = 0) {
    const pattern = `%${query.trim()}%`
    const where = `policy_state='approved' AND (name ILIKE $1 OR files_text ILIKE $1)`
    const [count, result] = await Promise.all([
      this.pool.query(`SELECT count(*)::bigint AS count FROM content WHERE ${where}`, [pattern]),
      this.pool.query(`SELECT content_id, info_hash, variant, name, total_size, file_count, updated_at
        FROM content WHERE ${where}
        ORDER BY CASE WHEN name ILIKE $1 THEN 0 ELSE 1 END, updated_at DESC
        LIMIT $2 OFFSET $3`, [pattern, limit, offset])
    ])
    return { results: normalizeContent(result.rows), total: Number(count.rows[0].count) }
  }

  async close() {
    this.closed = true
    await this.flushEvents()
    await this.pool.end()
  }
}

function normalizeContent(rows) {
  return rows.map((row) => ({
    ...row,
    total_size: Number(row.total_size),
    updated_at: row.updated_at.toISOString()
  }))
}

async function openPostgresCatalog(connectionString = process.env.DATABASE_URL) {
  const configuredPoolSize = Number(process.env.PGPOOL_MAX || 5)
  const max = Number.isSafeInteger(configuredPoolSize) && configuredPoolSize > 0 ? configuredPoolSize : 5
  const pool = new Pool({ ...connectionOptions(connectionString), max, idleTimeoutMillis: 10_000 })
  pool.on('error', (error) => process.stderr.write(`PostgreSQL idle connection error: ${error.message}\n`))
  const catalog = new PostgresCatalog(pool)
  try {
    return await catalog.initialize()
  } catch (error) {
    await pool.end()
    throw error
  }
}

export { PostgresCatalog, connectionOptions, openPostgresCatalog, schema }
