import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { DatabaseSync } from 'node:sqlite'
import { claimDiscoveredResource, claimMetadataJobs, compactEvent, completeMetadataJob, countDiscoveredResources, dashboardData, dashboardSummary, dashboardTrend, hasDiscoveredResource, ingestEvent, listCatalogPage, listRecentResourceObservations, markInvalidDiscoveredResources, openCatalog, queueMetadataJob, searchCatalog, searchCatalogPage, seedMetadataJobs, upsertManifest } from '../src/catalog.js'

test('catalog upserts manifests and searches names and file paths', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-catalog-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  try {
    assert.equal(upsertManifest(db, {
      info_hash: '0123456789abcdef0123456789abcdef01234567',
      variant: 'v1',
      name: 'Authorized Linux Image',
      total_size: 100,
      file_count: 1,
      metadata_size: 200,
      metadata_sha256: 'abc',
      files: [{ path: 'releases/linux-image.iso', size: 100 }]
    }), true)
    assert.equal(searchCatalog(db, 'Linux', 10)[0].name, 'Authorized Linux Image')
    assert.equal(searchCatalog(db, 'linux-image.iso', 10)[0].info_hash, '0123456789abcdef0123456789abcdef01234567')
    for (let i = 1; i <= 2; i++) {
      assert.equal(upsertManifest(db, {
        info_hash: String(i).repeat(40),
        variant: 'v1',
        name: `Authorized Linux Image ${i}`,
        total_size: 100 + i,
        file_count: 1,
        metadata_size: 200,
        metadata_sha256: `sha-${i}`,
        files: [{ path: `releases/linux-image-${i}.iso`, size: 100 + i }]
      }), true)
    }
    const firstPage = searchCatalogPage(db, 'Linux', 2, 0)
    const secondPage = searchCatalogPage(db, 'Linux', 2, 2)
    assert.equal(firstPage.total, 3)
    assert.equal(firstPage.results.length, 2)
    assert.equal(secondPage.results.length, 1)
    assert.equal(listCatalogPage(db, 2, 0).total, 3)
    assert.equal(listCatalogPage(db, 2, 2).results.length, 1)
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog stores probe events for dashboard summaries', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-probes-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  try {
    assert.equal(ingestEvent(db, {
      event_id: 'probe-1',
      schema_version: 1,
      event: 'dht.peer_discovered',
      occurred_at: '2026-08-08T12:00:00.000Z',
      info_hash: 'abcdefabcdefabcdefabcdefabcdefabcdefabcd',
      peer: { host: '192.0.2.10', port: 51413 },
      mode: 'lookup'
    }), true)
    assert.deepEqual(dashboardSummary(db), {
      content: 0,
      files: 0,
      probes: 1,
      peers: 1,
      lookups: 0,
      discovered: 0,
      active_discovered: 0,
      invalid_discovered: 0,
      last_event_at: '2026-08-08T12:00:00.000Z'
    })
    assert.equal(dashboardData(db, 10).probes[0].peer_host, '192.0.2.10')
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog marks resources invalid after seven days and reactivates them', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-resource-state-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  const oldHash = 'a'.repeat(40)
  const recentHash = 'b'.repeat(40)
  const now = new Date('2026-08-09T12:00:00.000Z')
  const oldSeen = new Date(now.getTime() - (8 * 24 * 60 * 60 * 1000)).toISOString()
  const recentSeen = new Date(now.getTime() - (2 * 24 * 60 * 60 * 1000)).toISOString()
  const cutoff = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000)).toISOString()
  try {
    assert.equal(claimDiscoveredResource(db, oldHash, oldSeen, 'get_peers'), true)
    assert.equal(claimDiscoveredResource(db, recentHash, recentSeen, 'announce_peer'), true)
    assert.equal(markInvalidDiscoveredResources(db, cutoff), 1)
    assert.equal(db.prepare('SELECT state FROM discovered_resource WHERE info_hash = ?').get(oldHash).state, 'invalid')
    assert.equal(db.prepare('SELECT state FROM discovered_resource WHERE info_hash = ?').get(recentHash).state, 'active')

    assert.equal(claimDiscoveredResource(db, oldHash, now.toISOString(), 'get_peers'), false)
    const reactivated = db.prepare('SELECT last_seen_at, state FROM discovered_resource WHERE info_hash = ?').get(oldHash)
    assert.equal(reactivated.last_seen_at, now.toISOString())
    assert.equal(reactivated.state, 'active')
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog loads only recent active resource observations for the collector cache', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-resource-cache-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  const oldHash = '1'.repeat(40)
  const recentHash = '2'.repeat(40)
  const invalidRecentHash = '3'.repeat(40)
  try {
    claimDiscoveredResource(db, oldHash, '2026-08-10T00:00:00.000Z', 'get_peers')
    claimDiscoveredResource(db, recentHash, '2026-08-12T12:00:00.000Z', 'get_peers')
    claimDiscoveredResource(db, invalidRecentHash, '2026-08-12T12:00:00.000Z', 'get_peers')
    db.prepare("UPDATE discovered_resource SET state = 'invalid' WHERE info_hash = ?").run(invalidRecentHash)

    const rows = listRecentResourceObservations(db, '2026-08-11T12:00:00.000Z')
    assert.deepEqual(rows, [{ info_hash: recentHash, last_seen_at: '2026-08-12T12:00:00.000Z' }])
    assert.equal(countDiscoveredResources(db), 3)
    assert.equal(hasDiscoveredResource(db, oldHash), true)
    assert.equal(hasDiscoveredResource(db, '4'.repeat(40)), false)
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog migrates legacy discovered resources without losing history', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-resource-migration-'))
  const dbPath = path.join(dir, 'catalog.db')
  const legacy = new DatabaseSync(dbPath)
  legacy.exec(`
    CREATE TABLE discovered_resource (
      info_hash TEXT PRIMARY KEY,
      first_seen_at TEXT NOT NULL,
      source TEXT NOT NULL
    );
    INSERT INTO discovered_resource (info_hash, first_seen_at, source)
    VALUES ('cccccccccccccccccccccccccccccccccccccccc', '2026-08-01T00:00:00.000Z', 'get_peers');
  `)
  legacy.close()

  const db = openCatalog(dbPath)
  try {
    const migrated = db.prepare('SELECT first_seen_at, last_seen_at, state FROM discovered_resource').get()
    assert.equal(migrated.first_seen_at, '2026-08-01T00:00:00.000Z')
    assert.equal(migrated.last_seen_at, '2026-08-01T00:00:00.000Z')
    assert.equal(migrated.state, 'active')
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog builds a five-minute trend from event buckets', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-trend-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  try {
    const now = Date.now()
    const events = [
      ['trend-link', 'dht.resource_discovered'],
      ['trend-query', 'dht.query_summary'],
      ['trend-failure', 'metadata.fetch_failed'],
      ['trend-warning', 'dht.warning']
    ]
    for (const [event_id, event] of events) ingestEvent(db, {
      event_id,
      schema_version: 1,
      event,
      occurred_at: new Date(now - 30_000).toISOString(),
      ...(event === 'dht.warning' ? { occurrences: 4 } : {}),
      ...(event === 'dht.query_summary' ? { occurrences: 12 } : {})
    })
    const trend = dashboardTrend(db, 5)
    assert.equal(trend.buckets.length, 5)
    assert.equal(trend.buckets.reduce((sum, bucket) => sum + bucket.links, 0), 1)
    assert.equal(trend.buckets.reduce((sum, bucket) => sum + bucket.queries, 0), 12)
    assert.equal(trend.buckets.reduce((sum, bucket) => sum + bucket.failures, 0), 1)
    assert.equal(trend.buckets.reduce((sum, bucket) => sum + bucket.warnings, 0), 4)
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog persists metadata jobs across retries and removes successful jobs', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-metadata-jobs-'))
  const db = openCatalog(path.join(dir, 'catalog.db'))
  const hash = 'd'.repeat(40)
  const observedAt = '2026-08-12T00:00:00.000Z'
  try {
    claimDiscoveredResource(db, hash, observedAt, 'get_peers')
    assert.equal(seedMetadataJobs(db, observedAt), 1)
    assert.deepEqual(claimMetadataJobs(db, 1, observedAt).map((job) => job.info_hash), [hash])
    assert.equal(claimMetadataJobs(db, 1, observedAt).length, 0)

    assert.equal(completeMetadataJob(db, hash, false, observedAt), true)
    assert.equal(claimMetadataJobs(db, 1, '2026-08-12T00:59:59.000Z').length, 0)
    assert.equal(claimMetadataJobs(db, 1, '2026-08-12T01:00:00.000Z').length, 1)
    assert.equal(completeMetadataJob(db, hash, true, '2026-08-12T01:00:01.000Z'), true)
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM metadata_job').get().count, 0)

    assert.equal(queueMetadataJob(db, hash, observedAt), true)
  } finally {
    db.close()
    fs.rmSync(dir, { recursive: true, force: true })
  }
})

test('catalog compacts metadata event payloads without changing the manifest', () => {
  const event = {
    event: 'metadata.fetch_completed',
    manifest: { info_hash: 'e'.repeat(40), name: 'Test', files: [{ path: 'large-file', size: 1 }] }
  }
  const compacted = compactEvent(event)
  assert.deepEqual(compacted.manifest, { info_hash: 'e'.repeat(40), name: 'Test' })
  assert.equal(event.manifest.files.length, 1)
})
