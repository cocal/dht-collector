import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import bencode from 'bencode'
import { ingestEvent, openCatalog } from './catalog.js'
import { parseManifest } from './metadata-fetcher.js'

function usage() {
  process.stderr.write(`Usage:\n  node src/authorized-torrent-import.js --input <file.torrent> --authorization-ref <reference> [--db <file>]\n\nThe importer validates the v1 infohash from a local authorized torrent metainfo file. It does not download payload data.\n`)
}

function parseArgs(argv) {
  const options = { input: null, authorizationRef: null, db: './var/dht-search.db' }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    const value = argv[++i]
    if (arg === '--input') options.input = value
    else if (arg === '--authorization-ref') options.authorizationRef = value?.trim()
    else if (arg === '--db') options.db = value
    else throw new Error(`unknown option: ${arg}`)
  }
  if (!options.input) throw new Error('--input is required')
  if (!options.authorizationRef) throw new Error('--authorization-ref is required')
  return options
}

function manifestFromTorrent(torrent) {
  let decoded
  try {
    decoded = bencode.decode(torrent)
  } catch (error) {
    throw new Error(`invalid torrent metainfo: ${error.message}`)
  }
  if (!decoded?.info || typeof decoded.info !== 'object') throw new Error('torrent metainfo is missing an info dictionary')
  const infoHash = crypto.createHash('sha1').update(bencode.encode(decoded.info)).digest('hex')
  return parseManifest(torrent, infoHash)
}

async function main() {
  let options
  try {
    options = parseArgs(process.argv.slice(2))
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    usage()
    process.exitCode = 2
    return
  }

  let manifest
  try {
    manifest = manifestFromTorrent(fs.readFileSync(path.resolve(options.input)))
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    process.exitCode = 2
    return
  }

  const event = {
    event_id: crypto.randomUUID(),
    schema_version: 1,
    event: 'metadata.import_completed',
    occurred_at: new Date().toISOString(),
    info_hash: manifest.info_hash,
    authorization_ref: options.authorizationRef,
    metadata_source: 'authorized-torrent-file',
    manifest
  }
  const db = openCatalog(options.db)
  try {
    ingestEvent(db, event)
  } finally {
    db.close()
  }
  process.stdout.write(`${JSON.stringify(event)}\n`)
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) main()

export { manifestFromTorrent }
