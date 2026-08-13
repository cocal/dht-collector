import crypto from 'node:crypto'
import fs from 'node:fs'
import net from 'node:net'
import path from 'node:path'
import process from 'node:process'
import readline from 'node:readline'
import bencode from 'bencode'
import Protocol from 'bittorrent-protocol'
import chardet from 'chardet'
import utMetadata from 'ut_metadata'

const MAX_METADATA_BYTES = 2 * 1024 * 1024
const ENCODING_PRIORITY = new Map([
  ['GB18030', 0],
  ['Big5', 1],
  ['Shift_JIS', 2],
  ['EUC-JP', 3],
  ['EUC-KR', 4]
])

function usage() {
  process.stderr.write('  --db <file>              Write worker events and manifests to SQLite' + String.fromCharCode(10))
  process.stderr.write(`Usage:\n  node src/metadata-fetcher.js --input <events.jsonl> [options]\n  cat events.jsonl | node src/metadata-fetcher.js --input - [options]\n\nOptions:\n  --input <file>           JSONL event log or - for stdin\n  --output <file>          Append worker events to a file as well as stdout\n  --timeout-ms <ms>        Per-peer timeout (default: 10000)\n  --max-per-infohash <n>   Attempts per infohash (default: 3)\n`)
}

function parseArgs(argv) {
  const options = {
    input: null,
    output: null,
    db: null,
    timeoutMs: 10000,
    maxPerInfohash: 3
  }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    const value = argv[++i]
    if (arg === '--input') options.input = value
    else if (arg === '--output') options.output = value
    else if (arg === '--db') options.db = value
    else if (arg === '--timeout-ms') options.timeoutMs = positiveInt(value, arg)
    else if (arg === '--max-per-infohash') options.maxPerInfohash = positiveInt(value, arg)
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

function createEmitter(outputPath, db, ingestStoredEvent) {
  const resolved = outputPath ? path.resolve(outputPath) : null
  if (resolved) fs.mkdirSync(path.dirname(resolved), { recursive: true })
  return (event, payload = {}) => {
    const record = {
      event_id: crypto.randomUUID(),
      schema_version: 1,
      event,
      occurred_at: new Date().toISOString(),
      ...payload
    }
    const line = JSON.stringify(record)
    if (db) ingestStoredEvent(db, record)
    process.stdout.write(`${line}\n`)
    if (resolved) fs.appendFileSync(resolved, `${line}\n`, 'utf8')
  }
}

function inputStream(input) {
  return input === '-' ? process.stdin : fs.createReadStream(path.resolve(input), { encoding: 'utf8' })
}

function cleanText(value) {
  return value.normalize('NFC').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '').trim()
}

function decodeBuffer(value) {
  const buffer = Buffer.from(value)
  try {
    return cleanText(new TextDecoder('utf-8', { fatal: true }).decode(buffer))
  } catch {
    const candidates = chardet.analyse(buffer).sort((left, right) => {
      const confidence = right.confidence - left.confidence
      if (confidence) return confidence
      return (ENCODING_PRIORITY.get(left.name) ?? 100) - (ENCODING_PRIORITY.get(right.name) ?? 100)
    })
    for (const candidate of candidates) {
      try {
        const decoded = new TextDecoder(candidate.name, { fatal: true }).decode(buffer)
        if (decoded && !decoded.includes('\ufffd')) return cleanText(decoded)
      } catch {
        // Try the next detected encoding.
      }
    }
    return cleanText(buffer.toString('utf8'))
  }
}

function asText(value) {
  if (ArrayBuffer.isView(value)) return decodeBuffer(value)
  return typeof value === 'string' ? cleanText(value) : ''
}

function pathText(value) {
  if (!Array.isArray(value)) return asText(value)
  return value.map(asText).join('/')
}

function parseManifest(metadata, infoHash) {
  if (!ArrayBuffer.isView(metadata)) {
    throw new Error(`metadata size must be between 1 and ${MAX_METADATA_BYTES} bytes`)
  }
  metadata = Buffer.from(metadata)
  if (metadata.length === 0 || metadata.length > MAX_METADATA_BYTES) {
    throw new Error(`metadata size must be between 1 and ${MAX_METADATA_BYTES} bytes`)
  }
  let info
  try {
    const decoded = bencode.decode(metadata)
    info = decoded && decoded.info ? decoded.info : decoded
    const infoBytes = decoded && decoded.info ? bencode.encode(info) : metadata
    const actualHash = crypto.createHash('sha1').update(infoBytes).digest('hex')
    if (actualHash !== infoHash) throw new Error(`metadata hash mismatch: expected ${infoHash}, got ${actualHash}`)
  } catch (error) {
    if (error.message.startsWith('metadata hash mismatch:')) throw error
    throw new Error(`invalid bencode metadata: ${error.message}`)
  }
  if (!info || typeof info !== 'object' || Array.isArray(info)) throw new Error('metadata info dictionary is missing')

  const name = asText(info['name.utf-8'] || info.name)
  const files = Array.isArray(info.files)
    ? info.files.map((file) => ({ path: pathText(file['path.utf-8'] || file.path), size: Number(file.length) }))
    : [{ path: name, size: Number(info.length || 0) }]
  if (!files.length || files.some((file) => !file.path || !Number.isSafeInteger(file.size) || file.size < 0)) {
    throw new Error('metadata contains an invalid file list')
  }

  return {
    info_hash: infoHash,
    variant: info['meta version'] === 2 ? 'v2-or-hybrid' : 'v1',
    name,
    total_size: files.reduce((total, file) => total + file.size, 0),
    file_count: files.length,
    files: files.slice(0, 10000),
    metadata_size: metadata.length,
    metadata_sha256: crypto.createHash('sha256').update(metadata).digest('hex')
  }
}

function fetchMetadata(infoHash, peer, timeoutMs, signal = null) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket()
    const wire = new Protocol()
    let settled = false
    let deadline
    const abortHandler = () => finish(Object.assign(new Error('metadata fetch aborted'), { code: 'ABORT_ERR' }))
    const localPeerId = crypto.randomBytes(20)

    const finish = (error, metadata) => {
      if (settled) return
      settled = true
      clearTimeout(deadline)
      signal?.removeEventListener('abort', abortHandler)
      socket.destroy()
      if (error) reject(error)
      else resolve(metadata)
    }

    const peerClosed = () => finish(new Error('peer closed connection before metadata'))
    deadline = setTimeout(() => finish(new Error(`peer timeout after ${timeoutMs}ms`)), timeoutMs)
    if (signal) {
      if (signal.aborted) return abortHandler()
      signal.addEventListener('abort', abortHandler, { once: true })
    }
    socket.once('error', (error) => finish(error))
    socket.once('end', peerClosed)
    socket.once('close', peerClosed)
    wire.once('error', (error) => finish(error))
    wire.use(utMetadata())
    wire.ut_metadata.once('metadata', (metadata) => finish(null, metadata))
    wire.ut_metadata.once('warning', (error) => finish(error))
    wire.on('handshake', (remoteInfoHash, remotePeerId, extensions) => {
      const remoteHash = Buffer.isBuffer(remoteInfoHash) ? remoteInfoHash.toString('hex') : remoteInfoHash
      if (remoteHash !== infoHash) return finish(new Error(`peer returned unexpected infohash ${remoteHash}`))
      if (!extensions?.extended) return finish(new Error('peer does not support BEP 10 extensions'))
      wire.ut_metadata.fetch()
    })

    socket.pipe(wire).pipe(socket)
    socket.connect(peer.port, peer.host, () => {
      wire.handshake(Buffer.from(infoHash, 'hex'), localPeerId, { dht: false })
    })
  })
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

  const sqlite = options.db ? await import('./catalog.js') : null
  const db = sqlite ? sqlite.openCatalog(options.db) : null
  const emit = createEmitter(options.output, db, sqlite?.ingestEvent)
  const attempts = new Map()
  const lines = readline.createInterface({ input: inputStream(options.input), crlfDelay: Infinity })
  emit('metadata.worker_ready', { input: options.input })

  try {
    for await (const line of lines) {
      if (!line.trim()) continue
      let event
      try {
        event = JSON.parse(line)
      } catch {
        continue
      }
      if (event.event !== 'dht.peer_discovered' || !event.info_hash || !event.peer) continue
      const infoHash = String(event.info_hash).toLowerCase()
      if (!/^[a-f0-9]{40}$/.test(infoHash)) continue
      const count = attempts.get(infoHash) || 0
      if (count >= options.maxPerInfohash) continue
      attempts.set(infoHash, count + 1)
      emit('metadata.fetch_started', { info_hash: infoHash, peer: event.peer, attempt: count + 1 })
      try {
        const metadata = await fetchMetadata(infoHash, event.peer, options.timeoutMs)
        const manifest = parseManifest(metadata, infoHash)
        emit('metadata.fetch_completed', { info_hash: infoHash, peer: event.peer, manifest })
      } catch (error) {
        emit('metadata.fetch_failed', { info_hash: infoHash, peer: event.peer, message: error.message })
      }
    }
  } finally {
    if (db) db.close()
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  main()
}

export { fetchMetadata, parseManifest }
