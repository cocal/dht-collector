import assert from 'node:assert/strict'
import crypto from 'node:crypto'
import { once } from 'node:events'
import fs from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { spawn } from 'node:child_process'
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import bencode from 'bencode'
import Protocol from 'bittorrent-protocol'
import utMetadata from 'ut_metadata'
import { dashboardSummary, openCatalog } from '../src/catalog.js'
import { fetchMetadata, parseManifest } from '../src/metadata-fetcher.js'

function infoFixture() {
  return {
    name: Buffer.from('authorized-fixture.txt'),
    length: 42,
    'piece length': 16384,
    pieces: Buffer.alloc(20)
  }
}

test('parseManifest validates the wrapped BEP 9 infohash', () => {
  const info = infoFixture()
  const infoBytes = bencode.encode(info)
  const metadata = bencode.encode({ info })
  const infoHash = crypto.createHash('sha1').update(infoBytes).digest('hex')
  const manifest = parseManifest(metadata, infoHash)

  assert.equal(manifest.info_hash, infoHash)
  assert.equal(manifest.name, 'authorized-fixture.txt')
  assert.equal(manifest.total_size, 42)
  assert.equal(manifest.file_count, 1)
})

test('parseManifest detects legacy text encodings without replacement characters', () => {
  const info = {
    name: Buffer.from('b2e2cad4', 'hex'),
    length: 7,
    'piece length': 16384,
    pieces: Buffer.alloc(20)
  }
  const infoBytes = bencode.encode(info)
  const infoHash = crypto.createHash('sha1').update(infoBytes).digest('hex')
  const manifest = parseManifest(infoBytes, infoHash)

  assert.equal(manifest.name, '测试')
  assert.equal(manifest.files[0].path, '测试')
})

test('metadata fetcher requests BEP 9 metadata without piece data', async () => {
  const info = infoFixture()
  const infoBytes = bencode.encode(info)
  const infoHash = crypto.createHash('sha1').update(infoBytes).digest('hex')
  const server = net.createServer((socket) => {
    const wire = new Protocol()
    wire.use(utMetadata(infoBytes))
    socket.pipe(wire).pipe(socket)
    wire.on('handshake', () => {
      wire.handshake(Buffer.from(infoHash, 'hex'), crypto.randomBytes(20), { dht: false })
    })
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = server.address().port
  try {
    const metadata = await fetchMetadata(infoHash, { host: '127.0.0.1', port }, 2000)
    const manifest = parseManifest(metadata, infoHash)
    assert.equal(manifest.name, 'authorized-fixture.txt')
  } finally {
    await new Promise((resolve) => server.close(resolve))
  }
})

test('metadata fetcher rejects when a peer closes without an error', async () => {
  const infoHash = 'a'.repeat(40)
  let peerSocket
  const server = net.createServer((socket) => {
    peerSocket = socket
    socket.end()
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = server.address().port
  try {
    await assert.rejects(
      fetchMetadata(infoHash, { host: '127.0.0.1', port }, 500),
      /peer (closed connection before metadata|timeout after 500ms)/
    )
  } finally {
    peerSocket?.destroy()
    await new Promise((resolve) => server.close(resolve))
  }
})

test('metadata worker persists its own events when a database is configured', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dht-worker-db-'))
  const dbPath = path.join(dir, 'catalog.db')
  const script = fileURLToPath(new URL('../src/metadata-fetcher.js', import.meta.url))
  const worker = spawn(process.execPath, [script, '--input', '-', '--db', dbPath], { stdio: ['pipe', 'ignore', 'pipe'] })
  worker.stdin.end()
  const [code] = await once(worker, 'close')
  try {
    assert.equal(code, 0)
    const db = openCatalog(dbPath)
    try {
      assert.equal(dashboardSummary(db).probes, 1)
    } finally {
      db.close()
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
})
