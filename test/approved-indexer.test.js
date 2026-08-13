import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { loadApprovedTargets, parseApprovedTarget } from '../src/approved-indexer.js'

test('approved indexer requires an infohash and authorization reference', () => {
  assert.deepEqual(
    parseApprovedTarget({
      info_hash: '0123456789abcdef0123456789abcdef01234567',
      authorization_ref: 'publisher-release-2026-08'
    }, 1),
    {
      infoHash: '0123456789abcdef0123456789abcdef01234567',
      authorizationRef: 'publisher-release-2026-08'
    }
  )
  assert.throws(
    () => parseApprovedTarget({ info_hash: '0123' }, 4),
    /line 4: info_hash must be a 40-character hexadecimal v1 hash/
  )
  assert.throws(
    () => parseApprovedTarget({ info_hash: '0123456789abcdef0123456789abcdef01234567' }, 5),
    /line 5: authorization_ref is required/
  )
})

test('approved indexer loads unique authorized targets from JSONL', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-approved-input-'))
  const input = path.join(dir, 'targets.jsonl')
  fs.writeFileSync(input, [
    '# approved publisher entries',
    '{"info_hash":"0123456789abcdef0123456789abcdef01234567","authorization_ref":"publisher-a"}',
    '{"info_hash":"89abcdef0123456789abcdef0123456789abcdef","authorization_ref":"publisher-b"}'
  ].join('\n'))
  try {
    assert.deepEqual(await loadApprovedTargets(input), [
      { infoHash: '0123456789abcdef0123456789abcdef01234567', authorizationRef: 'publisher-a' },
      { infoHash: '89abcdef0123456789abcdef0123456789abcdef', authorizationRef: 'publisher-b' }
    ])
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
})
