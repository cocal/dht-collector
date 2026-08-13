import crypto from 'node:crypto'
import { lookup as dnsLookup } from 'node:dns/promises'
import fs from 'node:fs'
import { isIP } from 'node:net'
import path from 'node:path'
import process from 'node:process'
import DHT from 'bittorrent-dht'

const DEFAULT_BOOTSTRAP = [
  'router.bittorrent.com:6881',
  'router.utorrent.com:6881',
  'dht.transmissionbt.com:6881'
]

function usage() {
  process.stderr.write('  --event-log <file>        Append all JSONL events to a local file' + String.fromCharCode(10))
  process.stderr.write('  --db <file>               Write probe events to SQLite' + String.fromCharCode(10))
  process.stderr.write(`Usage:\n  npm start -- health [--duration-ms 5000]\n  npm start -- lookup <40-hex-v1-infohash> [options]\n\nOptions:\n  --port <port>             UDP listen port (default: 0, ephemeral)\n  --address <ip>            Bind address (default: 0.0.0.0)\n  --timeout-ms <ms>         Lookup timeout (default: 30000)\n  --bootstrap <host:port,..> Override bootstrap nodes\n  --no-peer-address         Do not include discovered peer addresses\n`)
}

function parseArgs(argv) {
  const [command = 'health', ...args] = argv
  const value = command === 'lookup' ? args.shift() : undefined
  const rest = args
  const options = {
    port: 0,
    address: '0.0.0.0',
    timeoutMs: command === 'health' ? 5000 : 30000,
    bootstrap: DEFAULT_BOOTSTRAP,
    includePeerAddress: true,
    eventLog: null,
    db: null
  }

  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]
    if (arg === '--no-peer-address') {
      options.includePeerAddress = false
      continue
    }
    const next = rest[++i]
    if (arg === '--port') options.port = parsePort(next, '--port')
    else if (arg === '--address') options.address = next
    else if (arg === '--timeout-ms' || arg === '--duration-ms') options.timeoutMs = parsePositiveInt(next, arg)
    else if (arg === '--bootstrap') options.bootstrap = parseBootstrap(next)
    else if (arg === '--event-log') options.eventLog = next
    else if (arg === '--db') options.db = next
    else throw new Error(`unknown option: ${arg}`)
  }

  if (!['health', 'lookup'].includes(command)) throw new Error(`unknown command: ${command}`)
  if (command === 'lookup' && !/^[a-fA-F0-9]{40}$/.test(value || '')) {
    throw new Error('lookup requires a 40-character hexadecimal v1 infohash')
  }
  return { command, infoHash: value?.toLowerCase(), options }
}

function parsePositiveInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`)
  return number
}

function parsePort(value, name) {
  const number = Number(value)
  if (!Number.isInteger(number) || number < 0 || number > 65535) throw new Error(`${name} must be between 0 and 65535`)
  return number
}

function parseBootstrap(value) {
  if (!value) throw new Error('--bootstrap cannot be empty')
  return value.split(',').map((entry) => {
    const { host, port } = parseBootstrapEntry(entry)
    return `${host}:${port}`
  })
}

function parseBootstrapEntry(entry) {
  const value = entry.trim()
  const match = value.match(/^([^:]+)(?::(\d+))?$/)
  if (!match) throw new Error(`invalid bootstrap node: ${entry}`)
  const host = match[1]
  const port = match[2] || '6881'
  if (!host || !/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
    throw new Error(`invalid bootstrap node: ${entry}`)
  }
  return { host, port: Number(port) }
}

async function resolveBootstrap(entries, lookup = dnsLookup) {
  const resolved = new Set()
  for (const entry of entries) {
    const { host, port } = parseBootstrapEntry(entry)
    if (isIP(host) === 4) {
      resolved.add(`${host}:${port}`)
      continue
    }
    if (isIP(host) === 6) continue
    try {
      const records = await lookup(host, { family: 4, all: true, verbatim: true })
      for (const record of records) {
        if (record.family === 4) resolved.add(`${record.address}:${port}`)
      }
    } catch (error) {
      process.stderr.write(`bootstrap resolution failed for ${host}: ${error.message}\n`)
    }
  }
  if (resolved.size === 0) throw new Error('no IPv4 bootstrap addresses resolved')
  return [...resolved]
}

let eventLogPath = null
let eventStore = null
let ingestStoredEvent = null

function emit(type, payload = {}) {
  const event = {
    event_id: crypto.randomUUID(),
    schema_version: 1,
    event: type,
    occurred_at: new Date().toISOString(),
    ...payload
  }
  const line = JSON.stringify(event)
  if (eventStore) {
    try {
      ingestStoredEvent(eventStore, event)
    } catch (error) {
      process.stderr.write(`database write failed: ${error.message}\n`)
    }
  }
  if (eventLogPath) {
    try {
      fs.appendFileSync(eventLogPath, `${line}\n`, 'utf8')
    } catch (error) {
      process.stderr.write(`event log write failed: ${error.message}\n`)
    }
  }
  process.stdout.write(`${line}\n`)
}

function createDht(options, metrics) {
  const dht = new DHT({
    bootstrap: options.bootstrap,
    concurrency: 8
  })

  dht.on('node', () => { metrics.nodesSeen += 1 })
  dht.on('warning', (error) => emit('dht.warning', { message: error.message }))
  dht.on('error', (error) => emit('dht.error', { message: error.message }))
  return dht
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

async function runHealth(options) {
  const metrics = { nodesSeen: 0 }
  const dht = createDht(options, metrics)
  try {
    const address = await listen(dht, options)
    emit('collector.ready', {
      mode: 'health',
      listen: address,
      bootstrap_count: options.bootstrap.length
    })
    await new Promise((resolve) => setTimeout(resolve, options.timeoutMs))
    const snapshot = dht.toJSON()
    emit('collector.snapshot', {
      mode: 'health',
      routing_nodes: snapshot.nodes.length,
      nodes_seen: metrics.nodesSeen
    })
  } finally {
    await destroy(dht)
  }
}

async function runLookup(infoHash, options) {
  const metrics = { nodesSeen: 0, peersSeen: 0 }
  const dht = createDht(options, metrics)
  const peers = new Set()
  let abortLookup
  try {
    const address = await listen(dht, options)
    emit('collector.ready', {
      mode: 'lookup',
      info_hash: infoHash,
      listen: address,
      bootstrap_count: options.bootstrap.length
    })

    dht.on('peer', (peer, foundInfoHash, from) => {
      const key = `${peer.host}:${peer.port}`
      if (peers.has(key)) return
      peers.add(key)
      metrics.peersSeen += 1
      const event = {
        info_hash: Buffer.isBuffer(foundInfoHash) ? foundInfoHash.toString('hex') : foundInfoHash,
      }
      if (options.includePeerAddress) {
        event.peer = { host: peer.host, port: peer.port }
        if (from) event.discovered_from = { host: from.address, port: from.port }
      }
      emit('dht.peer_discovered', event)
    })

    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (abortLookup) abortLookup()
        reject(new Error(`lookup timed out after ${options.timeoutMs}ms`))
      }, options.timeoutMs)
      abortLookup = dht.lookup(infoHash, (error, nodesWithPeers) => {
        clearTimeout(timer)
        if (error) reject(error)
        else resolve(nodesWithPeers)
      })
    })

    emit('dht.lookup_completed', {
      info_hash: infoHash,
      nodes_with_peers: result,
      peers: metrics.peersSeen,
      nodes_seen: metrics.nodesSeen
    })
  } finally {
    await destroy(dht)
  }
}

function destroy(dht) {
  return new Promise((resolve) => dht.destroy(resolve))
}

async function main() {
  let parsed
  try {
    parsed = parseArgs(process.argv.slice(2))
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    usage()
    process.exitCode = 2
    return
  }

  if (parsed.options.eventLog) {
    eventLogPath = path.resolve(parsed.options.eventLog)
    fs.mkdirSync(path.dirname(eventLogPath), { recursive: true })
  }
  if (parsed.options.db) {
    const sqlite = await import('./catalog.js')
    eventStore = sqlite.openCatalog(parsed.options.db)
    ingestStoredEvent = sqlite.ingestEvent
  }

  try {
    parsed.options.bootstrap = await resolveBootstrap(parsed.options.bootstrap)
    if (parsed.command === 'health') await runHealth(parsed.options)
    else await runLookup(parsed.infoHash, parsed.options)
  } catch (error) {
    emit('collector.failed', { message: error.message })
    process.exitCode = 1
  } finally {
    if (eventStore) eventStore.close()
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) main()

export { DEFAULT_BOOTSTRAP, parseBootstrap, resolveBootstrap }
