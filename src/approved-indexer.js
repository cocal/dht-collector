import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import readline from 'node:readline'
import DHT from 'bittorrent-dht'
import { ingestEvent, openCatalog } from './catalog.js'
import { DEFAULT_BOOTSTRAP, parseBootstrap, resolveBootstrap } from './index.js'
import { fetchMetadata, parseManifest } from './metadata-fetcher.js'

function usage() {
  process.stderr.write(`Usage:\n  node src/approved-indexer.js --input <approved-infohashes.jsonl> [options]\n\nEach JSONL record must include a 40-character v1 info_hash and an authorization_ref.\n\nOptions:\n  --db <file>                 SQLite database (default: ./var/dht-search.db)\n  --event-log <file>          Append emitted events as JSONL\n  --port <port>               UDP listen port (default: 0)\n  --address <ip>              UDP bind address (default: 0.0.0.0)\n  --bootstrap <host:port,..>  Override DHT bootstrap nodes\n  --lookup-timeout-ms <ms>    Per-infohash DHT lookup timeout (default: 30000)\n  --metadata-timeout-ms <ms>  Per-peer metadata timeout (default: 10000)\n  --max-peers-per-infohash <n> Metadata attempts per approved infohash (default: 3)\n  --no-peer-address           Do not persist peer endpoint addresses\n`)
}

function parseArgs(argv) {
  const options = {
    input: null,
    db: './var/dht-search.db',
    eventLog: null,
    port: 0,
    address: '0.0.0.0',
    bootstrap: DEFAULT_BOOTSTRAP,
    lookupTimeoutMs: 30000,
    metadataTimeoutMs: 10000,
    maxPeersPerInfohash: 3,
    intervalMs: 0,
    includePeerAddress: true
  }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--no-peer-address') {
      options.includePeerAddress = false
      continue
    }
    const value = argv[++i]
    if (arg === '--input') options.input = value
    else if (arg === '--db') options.db = value
    else if (arg === '--event-log') options.eventLog = value
    else if (arg === '--port') options.port = parsePort(value)
    else if (arg === '--address') options.address = value
    else if (arg === '--bootstrap') options.bootstrap = parseBootstrap(value)
    else if (arg === '--lookup-timeout-ms') options.lookupTimeoutMs = positiveInt(value, arg)
    else if (arg === '--metadata-timeout-ms') options.metadataTimeoutMs = positiveInt(value, arg)
    else if (arg === '--max-peers-per-infohash') options.maxPeersPerInfohash = positiveInt(value, arg)
    else if (arg === '--interval-ms') options.intervalMs = positiveInt(value, arg)
    else throw new Error(`unknown option: ${arg}`)
  }
  if (!options.input) throw new Error('--input is required')
  return options
}

function positiveInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`)
  return number
}

function parsePort(value) {
  const port = Number(value)
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('--port must be between 0 and 65535')
  return port
}

function parseApprovedTarget(record, lineNumber) {
  const infoHash = String(record?.info_hash || '').toLowerCase()
  const authorizationRef = typeof record?.authorization_ref === 'string' ? record.authorization_ref.trim() : ''
  if (!/^[a-f0-9]{40}$/.test(infoHash)) throw new Error(`line ${lineNumber}: info_hash must be a 40-character hexadecimal v1 hash`)
  if (!authorizationRef) throw new Error(`line ${lineNumber}: authorization_ref is required`)
  return { infoHash, authorizationRef }
}

async function loadApprovedTargets(input) {
  const stream = fs.createReadStream(path.resolve(input), { encoding: 'utf8' })
  const lines = readline.createInterface({ input: stream, crlfDelay: Infinity })
  const targets = new Map()
  let lineNumber = 0
  for await (const line of lines) {
    lineNumber += 1
    if (!line.trim() || line.trimStart().startsWith('#')) continue
    let record
    try {
      record = JSON.parse(line)
    } catch (error) {
      throw new Error(`line ${lineNumber}: invalid JSON (${error.message})`)
    }
    const target = parseApprovedTarget(record, lineNumber)
    if (targets.has(target.infoHash)) throw new Error(`line ${lineNumber}: duplicate info_hash`)
    targets.set(target.infoHash, target)
  }
  if (targets.size === 0) throw new Error('input has no approved infohash records')
  return [...targets.values()]
}

function createEmitter(db, eventLogPath) {
  const resolvedLog = eventLogPath ? path.resolve(eventLogPath) : null
  if (resolvedLog) fs.mkdirSync(path.dirname(resolvedLog), { recursive: true })
  return (event, payload = {}) => {
    const record = {
      event_id: crypto.randomUUID(),
      schema_version: 1,
      event,
      occurred_at: new Date().toISOString(),
      ...payload
    }
    ingestEvent(db, record)
    if (resolvedLog) fs.appendFileSync(resolvedLog, `${JSON.stringify(record)}\n`, 'utf8')
    process.stdout.write(`${JSON.stringify(record)}\n`)
    return record
  }
}

function listen(dht, options) {
  return new Promise((resolve, reject) => {
    const onError = (error) => reject(error)
    dht.once('error', onError)
    dht.listen(options.port, options.address, () => {
      dht.removeListener('error', onError)
      resolve(dht.address())
    })
  })
}

function destroy(dht) {
  return new Promise((resolve) => dht.destroy(resolve))
}

function lookup(dht, infoHash, timeoutMs) {
  return new Promise((resolve, reject) => {
    let abort
    const timer = setTimeout(() => {
      if (abort) abort()
      reject(new Error(`lookup timed out after ${timeoutMs}ms`))
    }, timeoutMs)
    abort = dht.lookup(infoHash, (error, nodesWithPeers) => {
      clearTimeout(timer)
      if (error) reject(error)
      else resolve(nodesWithPeers)
    })
  })
}

async function indexTarget(dht, target, options, emit) {
  const peers = new Set()
  const metadataTasks = []
  const onPeer = (peer, foundInfoHash, from) => {
    const infoHash = Buffer.isBuffer(foundInfoHash) ? foundInfoHash.toString('hex') : String(foundInfoHash || '').toLowerCase()
    if (infoHash !== target.infoHash || peers.size >= options.maxPeersPerInfohash) return
    const key = `${peer.host}:${peer.port}`
    if (peers.has(key)) return
    peers.add(key)
    const payload = {
      mode: 'approved-index',
      info_hash: target.infoHash,
      authorization_ref: target.authorizationRef
    }
    if (options.includePeerAddress) {
      payload.peer = { host: peer.host, port: peer.port }
      if (from) payload.discovered_from = { host: from.address, port: from.port }
    }
    emit('dht.peer_discovered', payload)
    metadataTasks.push(fetchAndStoreMetadata(target, peer, options, emit))
  }

  dht.on('peer', onPeer)
  emit('authorized.lookup_started', {
    mode: 'approved-index',
    info_hash: target.infoHash,
    authorization_ref: target.authorizationRef
  })
  let nodesWithPeers
  let lookupError
  try {
    nodesWithPeers = await lookup(dht, target.infoHash, options.lookupTimeoutMs)
  } catch (error) {
    lookupError = error
    emit('authorized.lookup_failed', {
      mode: 'approved-index',
      info_hash: target.infoHash,
      authorization_ref: target.authorizationRef,
      message: error.message
    })
  } finally {
    await Promise.allSettled(metadataTasks)
    if (!lookupError) {
      emit('authorized.lookup_completed', {
        mode: 'approved-index',
        info_hash: target.infoHash,
        authorization_ref: target.authorizationRef,
        nodes_with_peers: nodesWithPeers,
        peers: peers.size,
        metadata_attempts: metadataTasks.length
      })
    }
    dht.removeListener('peer', onPeer)
  }
}

async function fetchAndStoreMetadata(target, peer, options, emit) {
  const base = {
    mode: 'approved-index',
    info_hash: target.infoHash,
    authorization_ref: target.authorizationRef
  }
  if (options.includePeerAddress) base.peer = { host: peer.host, port: peer.port }
  emit('metadata.fetch_started', base)
  try {
    const metadata = await fetchMetadata(target.infoHash, peer, options.metadataTimeoutMs)
    const manifest = parseManifest(metadata, target.infoHash)
    emit('metadata.fetch_completed', { ...base, manifest })
  } catch (error) {
    emit('metadata.fetch_failed', { ...base, message: error.message })
  }
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

  let targets
  try {
    targets = await loadApprovedTargets(options.input)
    options.bootstrap = await resolveBootstrap(options.bootstrap)
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    process.exitCode = 2
    return
  }

  const db = openCatalog(options.db)
  const emit = createEmitter(db, options.eventLog)
  const dht = new DHT({ bootstrap: options.bootstrap, concurrency: 8 })
  dht.on('warning', (error) => emit('dht.warning', { mode: 'approved-index', message: error.message }))
  dht.on('error', (error) => emit('dht.error', { mode: 'approved-index', message: error.message }))

  try {
    const address = await listen(dht, options)
    const initialTargets = targets
    emit('authorized.indexer_ready', {
      mode: 'approved-index',
      listen: address,
      bootstrap_count: options.bootstrap.length,
      approved_targets: initialTargets.length,
      interval_ms: options.intervalMs || null
    })
    let cycle = 0
    while (true) {
      cycle += 1
      const cycleTargets = await loadApprovedTargets(options.input)
      emit('authorized.indexer_cycle_started', {
        mode: 'approved-index',
        cycle,
        approved_targets: cycleTargets.length
      })
      for (const target of cycleTargets) await indexTarget(dht, target, options, emit)
      emit('authorized.indexer_cycle_completed', {
        mode: 'approved-index',
        cycle,
        approved_targets: cycleTargets.length
      })
      if (!options.intervalMs) break
      await new Promise((resolve) => setTimeout(resolve, options.intervalMs))
    }
  } finally {
    await destroy(dht)
    db.close()
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) main()

export { loadApprovedTargets, parseApprovedTarget }
