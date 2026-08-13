import test from 'node:test'
import assert from 'node:assert/strict'
import { RECENT_RESOURCE_TTL_MS, RecentResourceCache, asInfoHash, parseArgs } from '../src/passive-collector.js'

test('passive collector normalizes valid infohashes and rejects invalid values', () => {
  const hash = 'a'.repeat(40)
  assert.equal(asInfoHash(Buffer.from(hash, 'hex')), hash)
  assert.equal(asInfoHash('not-an-infohash'), null)
})

test('passive collector parses bounded discovery options', () => {
  const options = parseArgs([
    '--port', '51413',
    '--max-resources', '25',
    '--dht-nodes', '4',
    '--max-concurrent', '2',
    '--dht-concurrency', '24',
    '--max-pending', '96',
    '--no-peer-address'
  ])
  assert.equal(options.port, 51413)
  assert.equal(options.dhtNodes, 4)
  assert.equal(options.maxResources, 25)
  assert.equal(options.maxConcurrent, 2)
  assert.equal(options.dhtConcurrency, 24)
  assert.equal(options.maxPending, 96)
  assert.equal(options.includePeerAddress, false)
})

test('passive collector treats a zero resource limit as unlimited', () => {
  assert.equal(parseArgs(['--max-resources', '0']).maxResources, 0)
  assert.equal(parseArgs([]).maxResources, 0)
})

test('passive collector cache expires entries without retaining the full history', () => {
  const now = Date.parse('2026-08-13T00:00:00.000Z')
  const recent = 'a'.repeat(40)
  const old = 'b'.repeat(40)
  const cache = new RecentResourceCache(RECENT_RESOURCE_TTL_MS, [
    { info_hash: recent, last_seen_at: new Date(now - 1_000).toISOString() },
    { info_hash: old, last_seen_at: new Date(now - RECENT_RESOURCE_TTL_MS - 1).toISOString() }
  ], now)

  assert.equal(cache.size, 1)
  assert.equal(cache.has(recent, now), true)
  assert.equal(cache.has(old, now), false)
  cache.observe(old, now)
  assert.equal(cache.size, 2)
  assert.equal(cache.prune(now + RECENT_RESOURCE_TTL_MS + 1), 2)
  assert.equal(cache.size, 0)
})
