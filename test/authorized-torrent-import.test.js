import assert from 'node:assert/strict'
import crypto from 'node:crypto'
import test from 'node:test'
import bencode from 'bencode'
import { manifestFromTorrent } from '../src/authorized-torrent-import.js'

test('authorized torrent importer derives and validates the v1 infohash', () => {
  const info = {
    name: Buffer.from('authorized-release.iso'),
    length: 128,
    'piece length': 16384,
    pieces: Buffer.alloc(20)
  }
  const torrent = bencode.encode({ announce: Buffer.from('https://tracker.example/announce'), info })
  const manifest = manifestFromTorrent(torrent)

  assert.equal(manifest.info_hash, crypto.createHash('sha1').update(bencode.encode(info)).digest('hex'))
  assert.equal(manifest.name, 'authorized-release.iso')
  assert.equal(manifest.total_size, 128)
})
