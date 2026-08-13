import assert from 'node:assert/strict'
import test from 'node:test'
import { resolveBootstrap } from '../src/index.js'

test('resolveBootstrap expands hostname IPv4 records and retains direct IPv4 peers', async () => {
  const lookup = async (host, options) => {
    assert.equal(host, 'bootstrap.example')
    assert.deepEqual(options, { family: 4, all: true, verbatim: true })
    return [
      { address: '198.51.100.10', family: 4 },
      { address: '2001:db8::10', family: 6 },
      { address: '198.51.100.11', family: 4 }
    ]
  }

  assert.deepEqual(
    await resolveBootstrap(['bootstrap.example:6881', '203.0.113.5:7999'], lookup),
    ['198.51.100.10:6881', '198.51.100.11:6881', '203.0.113.5:7999']
  )
})

test('resolveBootstrap fails when no IPv4 bootstrap address is available', async () => {
  await assert.rejects(
    resolveBootstrap(['bootstrap.example:6881'], async () => []),
    /no IPv4 bootstrap addresses resolved/
  )
})
