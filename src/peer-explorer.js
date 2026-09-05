import crypto from 'node:crypto'
import process from 'node:process'
import DHT from 'bittorrent-dht'
import { Pool } from 'pg'
import { DEFAULT_BOOTSTRAP, resolveBootstrap } from './index.js'
import { connectionOptions } from './postgres-catalog.js'

const RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024

function positiveInt(value, name) {
  const number = Number(value)
  if (!Number.isSafeInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`)
  return number
}

function parseArgs(argv) {
  const options = {
    port: 51431,
    dhtNodes: 2,
    concurrency: 2,
    lookupTimeoutMs: 8_000,
    maxPeers: 8,
    pollMs: 500
  }
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index]
    const value = argv[++index]
    if (arg === '--port') options.port = positiveInt(value, arg)
    else if (arg === '--dht-nodes') options.dhtNodes = positiveInt(value, arg)
    else if (arg === '--concurrency') options.concurrency = positiveInt(value, arg)
    else if (arg === '--lookup-timeout-ms') options.lookupTimeoutMs = positiveInt(value, arg)
    else if (arg === '--max-peers') options.maxPeers = positiveInt(value, arg)
    else if (arg === '--poll-ms') options.pollMs = positiveInt(value, arg)
    else throw new Error(`unknown option: ${arg}`)
  }
  if (options.port + options.dhtNodes - 1 > 65535) throw new Error('DHT listen port range exceeds 65535')
  return options
}

function asInfoHash(value) {
  const infoHash = Buffer.isBuffer(value) ? value.toString('hex') : String(value || '').toLowerCase()
  return /^[a-f0-9]{40}$/.test(infoHash) ? infoHash : null
}

function listen(dht, port) {
  return new Promise((resolve, reject) => {
    const onError = (error) => reject(error)
    dht.once('error', onError)
    dht.listen(port, '0.0.0.0', () => {
      dht.removeListener('error', onError)
      try { dht._rpc.socket.socket.setRecvBufferSize(RECEIVE_BUFFER_BYTES) } catch { /* kernel default */ }
      resolve()
    })
  })
}

function destroy(dht) {
  return new Promise((resolve) => dht.destroy(resolve))
}

function lookup(dht, infoHash, timeoutMs) {
  return new Promise((resolve, reject) => {
    let finished = false
    let abort = null
    const finish = (error, value) => {
      if (finished) return
      finished = true
      clearTimeout(timer)
      if (error) reject(error)
      else resolve(value)
    }
    const timer = setTimeout(() => {
      if (abort) abort()
      finish(new Error(`lookup timed out after ${timeoutMs}ms`))
    }, timeoutMs)
    try {
      abort = dht.lookup(infoHash, (error, nodesWithPeers) => finish(error, nodesWithPeers))
    } catch (error) {
      finish(error)
    }
  })
}

async function recoverExpired(pool) {
  await pool.query(`
    WITH stale AS (
      SELECT info_hash FROM peer_exploration_job
      WHERE status='processing' AND locked_until <= now()
      ORDER BY locked_until FOR UPDATE SKIP LOCKED LIMIT 32
    )
    UPDATE peer_exploration_job j
    SET status='pending', locked_by=NULL, locked_until=NULL, next_attempt_at=now(), updated_at=now()
    FROM stale WHERE j.info_hash=stale.info_hash
  `)
}

async function claimJobs(pool, workerId, limit) {
  const { rows } = await pool.query(`
    WITH due AS (
      SELECT j.info_hash FROM peer_exploration_job j
      WHERE j.status='pending' AND j.next_attempt_at <= now()
        AND NOT EXISTS (SELECT 1 FROM content c WHERE c.info_hash=j.info_hash)
      ORDER BY j.priority DESC, j.next_attempt_at ASC, j.updated_at DESC
      FOR UPDATE OF j SKIP LOCKED LIMIT $2
    )
    UPDATE peer_exploration_job j
    SET attempts=j.attempts+1, status='processing', locked_by=$1,
      locked_until=now() + interval '30 seconds', updated_at=now()
    FROM due WHERE j.info_hash=due.info_hash
    RETURNING j.info_hash
  `, [workerId, limit])
  return rows.map((row) => row.info_hash)
}

async function completeJob(pool, workerId, infoHash, peers, error) {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    if (peers.length) {
      const now = new Date().toISOString()
      const ids = peers.map(() => crypto.randomUUID())
      await client.query(`
        INSERT INTO probe_event
          (event_id,event_type,occurred_at,info_hash,peer_host,peer_port,mode,message,raw_event)
        SELECT event_id,'dht.peer_discovered',$2::timestamptz,$3::text,peer_host,peer_port,
          'peer-explorer','isolated peer lookup',
          jsonb_build_object('event','dht.peer_discovered','mode','peer-explorer',
            'info_hash',$3::text,'peer',jsonb_build_object('host',peer_host,'port',peer_port))
        FROM unnest($1::text[],$4::text[],$5::integer[])
          AS input(event_id,peer_host,peer_port)
      `, [ids, now, infoHash, peers.map((peer) => peer.host), peers.map((peer) => peer.port)])
      await client.query(`
        INSERT INTO catalog_counter(name,value) VALUES('peers',$1)
        ON CONFLICT(name) DO UPDATE SET value=catalog_counter.value + excluded.value
      `, [peers.length])
      // Only wake a task that the collector has explicitly released for this
      // lookup. A processing metadata task remains owned by its collector.
      await client.query(`
        UPDATE metadata_job SET priority=greatest(priority,100), next_attempt_at=now(), updated_at=now()
        WHERE info_hash=$1 AND status='pending'
      `, [infoHash])
    }
    // A lookup is an opportunistic supplement to a live announce. Retrying an
    // empty lookup would turn noisy DHT traffic into an unbounded history queue;
    // a later direct miss can schedule a fresh, rate-limited lookup instead.
    await client.query(`DELETE FROM peer_exploration_job WHERE info_hash=$1 AND status='processing' AND locked_by=$2`, [infoHash, workerId])
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required')
  const bootstrap = await resolveBootstrap(DEFAULT_BOOTSTRAP)
  // The database role is shared with the collector, dashboard and monitor.
  // One connection is enough here because this worker has two bounded lookups
  // but only serial, short database state transitions.
  const pool = new Pool({
    ...connectionOptions(), max: 1, idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 3_000,
    options: '-c statement_timeout=3000 -c lock_timeout=1000'
  })
  const nodes = Array.from({ length: options.dhtNodes }, () => new DHT({ bootstrap: false, nodes: bootstrap, concurrency: 24 }))
  const collectors = new Map()
  const active = new Set()
  let stopping = false
  let nextNode = 0
  let lastWarningAt = 0
  let tickInFlight = null
  const workerId = `peer-explorer-${process.pid}`

  for (const node of nodes) {
    node.setMaxListeners(32)
    node.on('error', (error) => process.stderr.write(`DHT error: ${error.message}\n`))
    node.on('warning', (error) => {
      const now = Date.now()
      if (now - lastWarningAt >= 60_000) {
        lastWarningAt = now
        process.stderr.write(`DHT warning: ${error.message}\n`)
      }
    })
    node.on('peer', (peer, rawInfoHash) => {
      const infoHash = asInfoHash(rawInfoHash)
      const peers = infoHash && collectors.get(infoHash)
      if (!peers || !peer?.host || !Number.isInteger(peer.port) || peer.port < 1 || peer.port > 65535) return
      const key = `${peer.host}:${peer.port}`
      if (peers.size < options.maxPeers && !peers.has(key)) peers.set(key, { host: peer.host, port: peer.port })
    })
  }
  await Promise.all(nodes.map((node, index) => listen(node, options.port + index)))
  for (const node of nodes) node._bootstrap(true)
  process.stdout.write(`peer explorer ready: ${nodes.length} DHT nodes, concurrency ${options.concurrency}\n`)

  const run = async (infoHash) => {
    const peers = new Map()
    collectors.set(infoHash, peers)
    let failure = null
    try {
      await lookup(nodes[nextNode++ % nodes.length], infoHash, options.lookupTimeoutMs)
    } catch (error) {
      failure = error
    } finally {
      collectors.delete(infoHash)
    }
    await completeJob(pool, workerId, infoHash, [...peers.values()], failure?.message)
  }

  const tick = async () => {
    if (stopping) return
    try {
      await recoverExpired(pool)
      const jobs = await claimJobs(pool, workerId, Math.max(0, options.concurrency - active.size))
      for (const infoHash of jobs) {
        const task = run(infoHash).catch((error) => process.stderr.write(`peer lookup ${infoHash} failed: ${error.message}\n`))
          .finally(() => active.delete(task))
        active.add(task)
      }
    } catch (error) {
      process.stderr.write(`peer explorer queue error: ${error.message}\n`)
    }
  }
  const scheduleTick = () => {
    if (tickInFlight) return
    tickInFlight = tick().finally(() => { tickInFlight = null })
  }
  const timer = setInterval(scheduleTick, options.pollMs)
  timer.unref()
  scheduleTick()
  await new Promise((resolve) => {
    const stop = () => { stopping = true; clearInterval(timer); resolve() }
    process.once('SIGINT', stop)
    process.once('SIGTERM', stop)
  })
  if (tickInFlight) await tickInFlight
  await Promise.all(nodes.map(destroy))
  await Promise.allSettled(active)
  await pool.end()
}

if (process.argv[1] && new URL(import.meta.url).pathname === process.argv[1]) {
  main().catch((error) => {
    process.stderr.write(`peer explorer failed: ${error.stack || error.message}\n`)
    process.exitCode = 1
  })
}

export { asInfoHash, parseArgs }
