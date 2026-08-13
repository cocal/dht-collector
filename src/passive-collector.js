import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import DHT from 'bittorrent-dht'
import { compactEvent } from './event-utils.js'
import { DEFAULT_BOOTSTRAP, parseBootstrap, resolveBootstrap } from './index.js'
import { fetchMetadata, parseManifest } from './metadata-fetcher.js'
import { openStorage } from './storage.js'

const INVALID_AFTER_MS = 7 * 24 * 60 * 60 * 1000
const INVALID_SWEEP_MS = 60 * 60 * 1000
const RECENT_RESOURCE_TTL_MS = 24 * 60 * 60 * 1000
const RECENT_RESOURCE_SWEEP_MS = 60 * 60 * 1000
const QUERY_FLUSH_MS = 10 * 1000
const RESOURCE_TOUCH_MS = 30 * 1000
const JOB_POLL_MS = 250
const MAX_ANNOUNCED_RESOURCE_CACHE = 50_000
const UDP_RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024

function usage() {
  process.stderr.write(`Usage:\n  node src/passive-collector.js [options]\n\nListens for incoming Mainline DHT get_peers and announce_peer traffic.\n\nOptions:\n  --db <file>                 SQLite database (default: ./var/dht-search.db)\n  --event-log <file>          Append compact emitted events as JSONL\n  --quiet                     Do not mirror persisted events to stdout\n  --port <port>               First UDP listen port (default: 51413)\n  --dht-nodes <n>             Independent DHT identities on sequential ports (default: 1)\n  --address <ip>              UDP bind address (default: 0.0.0.0)\n  --bootstrap <host:port,..>  Override DHT bootstrap nodes\n  --lookup-timeout-ms <ms>    Lookup timeout for get_peers discoveries (default: 30000)\n  --metadata-timeout-ms <ms>  Per-peer metadata timeout (default: 12000)\n  --max-resources <n>         Resource limit; 0 means unlimited (default: 0)\n  --max-concurrent <n>        Concurrent lookup/metadata tasks (default: 16)\n  --dht-concurrency <n>       Concurrent UDP RPC requests per DHT node (default: 32)\n  --max-pending <n>           In-memory task queue bound (default: 128)\n  --max-peers-per-infohash <n> Metadata attempts per resource (default: 3)\n  --no-peer-address           Do not persist peer endpoint addresses\n`)
}

function positiveInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`)
  return number
}

function nonNegativeInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number < 0) throw new Error(`${name} must be a non-negative integer`)
  return number
}

function parsePort(value) {
  const port = Number(value)
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('--port must be between 0 and 65535')
  return port
}

function parseArgs(argv) {
  const options = {
    db: './var/dht-search.db',
    eventLog: null,
    quiet: false,
    port: 51413,
    dhtNodes: 1,
    address: '0.0.0.0',
    bootstrap: DEFAULT_BOOTSTRAP,
    lookupTimeoutMs: 30000,
    metadataTimeoutMs: 12000,
    maxResources: 0,
    maxConcurrent: 16,
    dhtConcurrency: 32,
    maxPending: 128,
    maxPeersPerInfohash: 3,
    includePeerAddress: true
  }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--no-peer-address') {
      options.includePeerAddress = false
      continue
    }
    if (arg === '--quiet') {
      options.quiet = true
      continue
    }
    const value = argv[++i]
    if (arg === '--db') options.db = value
    else if (arg === '--event-log') options.eventLog = value
    else if (arg === '--port') options.port = parsePort(value)
    else if (arg === '--dht-nodes') options.dhtNodes = positiveInt(value, arg)
    else if (arg === '--address') options.address = value
    else if (arg === '--bootstrap') options.bootstrap = parseBootstrap(value)
    else if (arg === '--lookup-timeout-ms') options.lookupTimeoutMs = positiveInt(value, arg)
    else if (arg === '--metadata-timeout-ms') options.metadataTimeoutMs = positiveInt(value, arg)
    else if (arg === '--max-resources') options.maxResources = nonNegativeInt(value, arg)
    else if (arg === '--max-concurrent') options.maxConcurrent = positiveInt(value, arg)
    else if (arg === '--dht-concurrency') options.dhtConcurrency = positiveInt(value, arg)
    else if (arg === '--max-pending') options.maxPending = positiveInt(value, arg)
    else if (arg === '--max-peers-per-infohash') options.maxPeersPerInfohash = positiveInt(value, arg)
    else throw new Error(`unknown option: ${arg}`)
  }
  if (options.port + options.dhtNodes - 1 > 65535) throw new Error('DHT listen port range exceeds 65535')
  return options
}

function asInfoHash(value) {
  const infoHash = Buffer.isBuffer(value) ? value.toString('hex') : String(value || '').toLowerCase()
  return /^[a-f0-9]{40}$/.test(infoHash) ? infoHash : null
}

class RecentResourceCache {
  constructor(ttlMs, observations = [], now = Date.now()) {
    this.ttlMs = ttlMs
    this.resources = new Map()
    for (const observation of observations) {
      const observedAt = Date.parse(observation.last_seen_at)
      if (Number.isFinite(observedAt)) this.resources.set(observation.info_hash, observedAt)
    }
    this.prune(now)
  }

  has(infoHash, now = Date.now()) {
    const observedAt = this.resources.get(infoHash)
    if (observedAt == null) return false
    if (observedAt < now - this.ttlMs) {
      this.resources.delete(infoHash)
      return false
    }
    return true
  }

  observe(infoHash, observedAt = Date.now()) {
    this.resources.set(infoHash, observedAt)
  }

  delete(infoHash) {
    this.resources.delete(infoHash)
  }

  prune(now = Date.now()) {
    const cutoff = now - this.ttlMs
    let removed = 0
    for (const [infoHash, observedAt] of this.resources) {
      if (observedAt >= cutoff) continue
      this.resources.delete(infoHash)
      removed += 1
    }
    return removed
  }

  get size() {
    return this.resources.size
  }
}

function createEmitter(catalog, eventLogPath, quiet = false) {
  const resolvedLog = eventLogPath ? path.resolve(eventLogPath) : null
  const pendingWrites = new Set()
  let closed = false
  if (resolvedLog) fs.mkdirSync(path.dirname(resolvedLog), { recursive: true })
  const emit = (event, payload = {}) => {
    const record = {
      event_id: crypto.randomUUID(),
      schema_version: 1,
      event,
      occurred_at: new Date().toISOString(),
      ...payload
    }
    if (closed) return record
    const write = Promise.resolve(catalog.ingestEvent(record))
    pendingWrites.add(write)
    write.then(() => pendingWrites.delete(write), (error) => {
      pendingWrites.delete(write)
      process.stderr.write(`event persistence failed: ${error.message}\n`)
    })
    if (resolvedLog || !quiet) {
      const line = JSON.stringify(compactEvent(record))
      if (resolvedLog) fs.appendFileSync(resolvedLog, `${line}\n`, 'utf8')
      if (!quiet) process.stdout.write(`${line}\n`)
    }
    return write
  }
  emit.close = async () => {
    closed = true
    if (catalog.flushEvents) await catalog.flushEvents()
    await Promise.allSettled(pendingWrites)
  }
  return emit
}

function listen(dht, options) {
  return new Promise((resolve, reject) => {
    const onError = (error) => reject(error)
    dht.once('error', onError)
    dht.listen(options.port, options.address, () => {
      dht.removeListener('error', onError)
      try {
        dht._rpc.socket.socket.setRecvBufferSize(UDP_RECEIVE_BUFFER_BYTES)
      } catch {
        // The kernel default remains valid when the socket does not expose this option.
      }
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

async function fetchAndStoreMetadata(infoHash, peer, options, emit, context = {}) {
  const base = {
    mode: 'passive',
    info_hash: infoHash,
    discovery: context.discovery || 'announce_peer'
  }
  if (options.includePeerAddress) base.peer = { host: peer.host, port: peer.port }
  if (!context.aggregate) emit('metadata.fetch_started', base)
  try {
    const metadata = await fetchMetadata(infoHash, peer, options.metadataTimeoutMs, context.signal)
    const manifest = parseManifest(metadata, infoHash)
    await emit('metadata.fetch_completed', { ...base, manifest })
    return true
  } catch (error) {
    if (error.code === 'ABORT_ERR') return false
    if (context.aggregate) context.errors.push(error.message)
    else emit('metadata.fetch_failed', { ...base, message: error.message })
    return false
  }
}

async function lookupAndFetch(dht, peerCollectors, infoHash, options, emit) {
  const peers = new Map()
  peerCollectors.set(infoHash, peers)
  emit('dht.lookup_started', { mode: 'passive', info_hash: infoHash })
  try {
    const nodesWithPeers = await lookup(dht, infoHash, options.lookupTimeoutMs)
    const controller = new AbortController()
    const errors = []
    const results = await Promise.all([...peers.values()].map((peer) => fetchAndStoreMetadata(infoHash, peer, options, emit, {
      aggregate: true,
      discovery: 'get_peers',
      errors,
      signal: controller.signal
    }).then((result) => {
      if (result) controller.abort()
      return result
    })))
    const succeeded = results.includes(true)
    if (results.length) emit('metadata.fetch_summary', {
      mode: 'passive',
      info_hash: infoHash,
      attempts: results.length,
      failures: succeeded ? errors.length : results.length,
      succeeded,
      message: succeeded ? null : errors[0] || 'no metadata-capable peer'
    })
    emit('dht.lookup_completed', {
      mode: 'passive',
      info_hash: infoHash,
      nodes_with_peers: nodesWithPeers,
      peers: peers.size
    })
    return succeeded
  } catch (error) {
    emit('dht.lookup_failed', { mode: 'passive', info_hash: infoHash, message: error.message })
    return false
  } finally {
    peerCollectors.delete(infoHash)
  }
}

async function main() {
  let options
  try {
    options = parseArgs(process.argv.slice(2))
    options.bootstrap = await resolveBootstrap(options.bootstrap)
  } catch (error) {
    process.stderr.write(`error: ${error.message}\n`)
    usage()
    process.exitCode = 2
    return
  }

  const catalog = await openStorage({ db: options.db })
  const emit = createEmitter(catalog, options.eventLog, options.quiet)
  const cacheStartedAt = Date.now()
  const cacheCutoff = new Date(cacheStartedAt - RECENT_RESOURCE_TTL_MS).toISOString()
  const [recentResourceObservations, discoveredCount] = await Promise.all([
    catalog.listRecentResourceObservations(cacheCutoff),
    options.maxResources > 0 ? catalog.countDiscoveredResources() : Promise.resolve(0)
  ])
  const recentResources = new RecentResourceCache(RECENT_RESOURCE_TTL_MS, recentResourceObservations, cacheStartedAt)
  const dhtNodes = Array.from({ length: options.dhtNodes }, () => {
    const node = new DHT({ bootstrap: false, nodes: options.bootstrap, concurrency: options.dhtConcurrency })
    node.setMaxListeners(32)
    return node
  })
  const pending = []
  const priorityPending = []
  const runningTasks = new Set()
  const announcedPeers = new Map()
  const warningState = new Map()
  const recentObservations = new Map()
  const queryCounts = new Map()
  const queuedJobs = new Set()
  const peerCollectors = new Map()
  let active = 0
  let stopping = false
  let discovered = discoveredCount
  let limitReported = false
  let droppedTasks = 0
  let nextDhtIndex = 0

  const collectPeer = (peer, rawInfoHash) => {
    const infoHash = asInfoHash(rawInfoHash)
    const peers = infoHash && peerCollectors.get(infoHash)
    if (!peers || !peer?.host || !peer?.port || peers.size >= options.maxPeersPerInfohash) return
    const key = `${peer.host}:${peer.port}`
    if (peers.has(key) || [...peers.values()].some((candidate) => candidate.host === peer.host)) return
    peers.set(key, peer)
  }
  for (const node of dhtNodes) node.on('peer', collectPeer)
  const nextDht = () => dhtNodes[nextDhtIndex++ % dhtNodes.length]

  const enqueue = (task, priority = false, infoHash = null) => {
    const queue = priority ? priorityPending : pending
    if (queue.length >= options.maxPending) {
      droppedTasks += 1
      return false
    }
    if (infoHash) queuedJobs.add(infoHash)
    queue.push({ task, infoHash })
    pump()
    return true
  }

  const pump = () => {
    while (active < options.maxConcurrent && (priorityPending.length || pending.length)) {
      const entry = priorityPending.shift() || pending.shift()
      active += 1
      const running = Promise.resolve(entry.task()).catch((error) => {
        void emit('passive.task_failed', { mode: 'passive', message: error.message })
      }).finally(() => {
        active -= 1
        if (entry.infoHash) queuedJobs.delete(entry.infoHash)
        runningTasks.delete(running)
        pump()
      })
      runningTasks.add(running)
    }
  }

  const acceptResource = async (infoHash, payload, task) => {
    if (!infoHash || stopping) return { known: false, newlyDiscovered: false }
    const observedAt = Date.now()
    const alreadyKnown = recentResources.has(infoHash, observedAt)
    const occurredAt = new Date().toISOString()
    if (alreadyKnown) {
      recentResources.observe(infoHash, observedAt)
      recentObservations.set(infoHash, occurredAt)
      return { known: true, newlyDiscovered: false }
    }
    if (!alreadyKnown && options.maxResources > 0 && discovered >= options.maxResources) {
      const existsInHistory = await catalog.hasDiscoveredResource(infoHash)
      if (!existsInHistory) {
        if (!limitReported) {
          emit('passive.resource_limit_reached', { mode: 'passive', max_resources: options.maxResources })
          limitReported = true
        }
        return { known: false, newlyDiscovered: false }
      }
    }
    recentResources.observe(infoHash, observedAt)
    let newlyDiscovered
    try {
      newlyDiscovered = await catalog.claimDiscoveredResource(infoHash, occurredAt, payload.discovery)
    } catch (error) {
      recentResources.delete(infoHash)
      throw error
    }
    if (!newlyDiscovered) return { known: true, newlyDiscovered: false }
    discovered += 1
    emit('dht.resource_discovered', { mode: 'passive', info_hash: infoHash, ...payload })
    if (task) {
      await catalog.queueMetadataJob(infoHash, occurredAt, 10)
      const job = await catalog.claimMetadataJob(infoHash, occurredAt)
      if (job) enqueue(async () => catalog.completeMetadataJob(infoHash, await task()), false, infoHash)
    }
    return { known: true, newlyDiscovered: true }
  }

  const invalidateExpiredResources = async () => {
    const cutoff = new Date(Date.now() - INVALID_AFTER_MS).toISOString()
    const count = await catalog.markInvalidDiscoveredResources(cutoff)
    if (count) emit('passive.resources_invalidated', { mode: 'passive', count, cutoff, inactive_days: 7 })
  }

  const enqueueAnnouncedPeer = async (infoHash, peer) => {
    const peerKey = `${peer.host}:${peer.port}`
    let peers = announcedPeers.get(infoHash)
    if (!peers) {
      if (announcedPeers.size >= MAX_ANNOUNCED_RESOURCE_CACHE) announcedPeers.delete(announcedPeers.keys().next().value)
      peers = new Set()
      announcedPeers.set(infoHash, peers)
    } else {
      announcedPeers.delete(infoHash)
      announcedPeers.set(infoHash, peers)
    }
    if (peers.has(peerKey) || peers.size >= options.maxPeersPerInfohash) return false
    peers.add(peerKey)
    const occurredAt = new Date().toISOString()
    await catalog.queueMetadataJob(infoHash, occurredAt, 100, true)
    const job = await catalog.claimMetadataJob(infoHash, occurredAt)
    if (job) enqueue(async () => catalog.completeMetadataJob(infoHash, await fetchAndStoreMetadata(infoHash, peer, options, emit)), true, infoHash)
    return true
  }

  const observeQuery = (query) => queryCounts.set(query, (queryCounts.get(query) || 0) + 1)

  const flushQueries = () => {
    for (const [query, occurrences] of queryCounts) {
      emit('dht.query_summary', { mode: 'passive', query, occurrences, interval_seconds: QUERY_FLUSH_MS / 1000 })
    }
    queryCounts.clear()
    if (droppedTasks) {
      emit('passive.queue_pressure', {
        mode: 'passive',
        dropped_tasks: droppedTasks,
        pending: pending.length,
        priority_pending: priorityPending.length
      })
      droppedTasks = 0
    }
  }

  const flushObservations = async () => {
    if (!recentObservations.size) return
    const observations = new Map(recentObservations)
    recentObservations.clear()
    await catalog.touchDiscoveredResources(observations)
  }

  let jobPollInFlight = false
  const pollMetadataJobs = async () => {
    if (stopping) return
    const capacity = Math.min(options.maxPending - pending.length, options.maxConcurrent - active)
    if (capacity <= 0) return
    if (jobPollInFlight) return
    jobPollInFlight = true
    try {
      const jobs = await catalog.claimMetadataJobs(capacity)
      for (const job of jobs) {
        if (queuedJobs.has(job.info_hash)) continue
        enqueue(async () => catalog.completeMetadataJob(
          job.info_hash,
          await lookupAndFetch(nextDht(), peerCollectors, job.info_hash, options, emit)
        ), false, job.info_hash)
      }
    } finally {
      jobPollInFlight = false
    }
  }

  const reportWarning = (error) => {
    const message = String(error?.message || error || 'unknown DHT warning')
    const now = Date.now()
    const previous = warningState.get(message)
    if (previous && now - previous.reportedAt < 60_000) {
      previous.suppressed += 1
      return
    }
    const occurrences = (previous?.suppressed || 0) + 1
    warningState.set(message, { reportedAt: now, suppressed: 0 })
    emit('dht.warning', { mode: 'passive', message, occurrences })
  }
  await invalidateExpiredResources()
  const seededJobs = await catalog.seedMetadataJobs()
  const runSafely = (operation) => void operation().catch((error) => {
    void emit('passive.task_failed', { mode: 'passive', message: error.message })
  })
  const invalidationTimer = setInterval(() => runSafely(invalidateExpiredResources), INVALID_SWEEP_MS)
  invalidationTimer.unref()
  const recentResourceTimer = setInterval(() => recentResources.prune(), RECENT_RESOURCE_SWEEP_MS)
  recentResourceTimer.unref()
  const queryTimer = setInterval(flushQueries, QUERY_FLUSH_MS)
  queryTimer.unref()
  const observationTimer = setInterval(() => runSafely(flushObservations), RESOURCE_TOUCH_MS)
  observationTimer.unref()
  const jobTimer = setInterval(() => runSafely(pollMetadataJobs), JOB_POLL_MS)
  jobTimer.unref()
  for (const node of dhtNodes) {
    node.on('warning', reportWarning)
    node.on('error', (error) => emit('dht.error', { mode: 'passive', message: error.message }))
    node.on('get_peers', (rawInfoHash) => {
      const infoHash = asInfoHash(rawInfoHash)
      if (!infoHash) return
      observeQuery('get_peers')
      runSafely(() => acceptResource(infoHash, { discovery: 'get_peers', query: 'get_peers' }, () => lookupAndFetch(node, peerCollectors, infoHash, options, emit)))
    })
    node.on('announce', (peer, rawInfoHash, from) => runSafely(async () => {
        const infoHash = asInfoHash(rawInfoHash)
        if (!infoHash || !peer?.host || !peer?.port) return
        const payload = { discovery: 'announce_peer' }
        if (options.includePeerAddress) {
          payload.peer = { host: peer.host, port: peer.port }
          if (from) payload.discovered_from = { host: from.host, port: from.port }
        }
        payload.query = 'announce_peer'
        observeQuery('announce_peer')
        const resource = await acceptResource(infoHash, payload)
        if (resource.known) await enqueueAnnouncedPeer(infoHash, peer)
      }))
  }

  try {
    const addresses = await Promise.all(dhtNodes.map((node, index) => listen(node, { ...options, port: options.port + index })))
    for (const node of dhtNodes) node._bootstrap(true)
    emit('passive.collector_ready', {
      mode: 'passive',
      listen: addresses,
      dht_nodes: dhtNodes.length,
      bootstrap_count: options.bootstrap.length,
      max_resources: options.maxResources,
      max_concurrent: options.maxConcurrent,
      dht_concurrency: options.dhtConcurrency,
      max_pending: options.maxPending,
      receive_buffer_bytes: dhtNodes[0]._rpc.socket.socket.getRecvBufferSize(),
      seeded_metadata_jobs: seededJobs,
      cached_resources: recentResources.size,
      resource_cache_hours: RECENT_RESOURCE_TTL_MS / (60 * 60 * 1000),
      invalid_after_days: 7
    })
    await new Promise((resolve) => {
      const stop = () => {
        stopping = true
        pending.length = 0
        priorityPending.length = 0
        for (const node of dhtNodes) void destroy(node)
        resolve()
      }
      process.once('SIGINT', stop)
      process.once('SIGTERM', stop)
    })
  } finally {
    clearInterval(invalidationTimer)
    clearInterval(recentResourceTimer)
    clearInterval(queryTimer)
    clearInterval(observationTimer)
    clearInterval(jobTimer)
    flushQueries()
    await flushObservations()
    await Promise.allSettled(runningTasks)
    await Promise.all(dhtNodes.map(destroy))
    await emit.close()
    await catalog.close()
    if (stopping) process.exit(0)
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) main()

export { RECENT_RESOURCE_TTL_MS, RecentResourceCache, asInfoHash, parseArgs }
