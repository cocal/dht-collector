import test from 'node:test'
import assert from 'node:assert/strict'
import { asInfoHash, parseArgs } from './peer-explorer.js'

test('peer explorer accepts only valid v1 info hashes', () => {
  assert.equal(asInfoHash(Buffer.alloc(20, 1)), '0101010101010101010101010101010101010101')
  assert.equal(asInfoHash('not-a-hash'), null)
})

test('peer explorer keeps queue concurrency bounded by configuration', () => {
  assert.deepEqual(parseArgs(['--concurrency', '2', '--lookup-timeout-ms', '8000']).concurrency, 2)
  assert.throws(() => parseArgs(['--concurrency', '0']))
})
