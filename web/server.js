import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import process from 'node:process'
import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { openStorage } from '../src/storage.js'

const root = path.dirname(fileURLToPath(import.meta.url))
const publicDir = path.join(root, 'public')
const execFileAsync = promisify(execFile)
const SNIFFER_SERVICE = 'dht-passive-collector.service'

function options(argv) {
  const result = { db: process.env.DHT_DB || './var/dht-search.db', host: '127.0.0.1', port: 4173 }
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i]
    const value = argv[++i]
    if (key === '--db') result.db = value
    else if (key === '--host') result.host = value
    else if (key === '--port') result.port = Number(value)
    else throw new Error(`unknown option: ${key}`)
  }
  if (!Number.isInteger(result.port) || result.port < 1 || result.port > 65535) throw new Error('--port must be between 1 and 65535')
  return result
}

function json(res, status, body) {
  const data = JSON.stringify(body)
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*'
  })
  res.end(data)
}

function mime(file) {
  return {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml'
  }[path.extname(file)] || 'application/octet-stream'
}

function serveStatic(res, pathname) {
  const relative = pathname === '/' ? 'index.html' : pathname.slice(1)
  const file = path.resolve(publicDir, relative)
  if (file !== publicDir && !file.startsWith(`${publicDir}${path.sep}`)) return json(res, 403, { error: 'forbidden' })
  try {
    const body = fs.readFileSync(file)
    res.writeHead(200, { 'content-type': mime(file), 'cache-control': 'no-cache' })
    res.end(body)
  } catch {
    json(res, 404, { error: 'not found' })
  }
}

async function snifferStatus() {
  try {
    const { stdout } = await execFileAsync('/usr/bin/systemctl', ['is-active', SNIFFER_SERVICE], { timeout: 5000 })
    const status = stdout.trim() || 'unknown'
    return { service: SNIFFER_SERVICE, status, enabled: status === 'active' }
  } catch (error) {
    const status = String(error.stdout || '').trim() || 'unknown'
    return { service: SNIFFER_SERVICE, status, enabled: status === 'active' }
  }
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = ''
    req.setEncoding('utf8')
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 16 * 1024) req.destroy(new Error('request body too large'))
    })
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {})
      } catch (error) {
        reject(new Error(`invalid JSON: ${error.message}`))
      }
    })
    req.on('error', reject)
  })
}

async function setSnifferState(enabled) {
  if (typeof enabled !== 'boolean') throw new Error('enabled must be boolean')
  await execFileAsync('/usr/bin/systemctl', [enabled ? 'start' : 'stop', SNIFFER_SERVICE], { timeout: 15000 })
  return snifferStatus()
}

function createServer(catalog) {
  return http.createServer(async (req, res) => {
    const requestUrl = new URL(req.url, 'http://localhost')
    try {
      if (requestUrl.pathname === '/api/health') return json(res, 200, { ok: true })
      if (requestUrl.pathname === '/api/sniffer') {
        if (req.method === 'GET') return json(res, 200, await snifferStatus())
        if (req.method === 'POST') {
          const body = await readJsonBody(req)
          return json(res, 200, await setSnifferState(body.enabled))
        }
        res.setHeader('allow', 'GET, POST')
        return json(res, 405, { error: 'method not allowed' })
      }
      if (requestUrl.pathname === '/api/dashboard') {
        const limit = Math.min(Number(requestUrl.searchParams.get('limit') || 80), 200)
        return json(res, 200, await catalog.dashboardData(Number.isInteger(limit) && limit > 0 ? limit : 80))
      }
      if (requestUrl.pathname === '/api/content') {
        const requestedPage = Number(requestUrl.searchParams.get('page') || 1)
        const requestedPageSize = Number(requestUrl.searchParams.get('page_size') || 20)
        const page = Number.isInteger(requestedPage) && requestedPage > 0 ? requestedPage : 1
        const pageSize = Number.isInteger(requestedPageSize) && requestedPageSize > 0 ? Math.min(requestedPageSize, 100) : 20
        const { results, total } = await catalog.listCatalogPage(pageSize, (page - 1) * pageSize)
        return json(res, 200, {
          page,
          page_size: pageSize,
          total,
          total_pages: Math.ceil(total / pageSize),
          results
        })
      }
      if (requestUrl.pathname === '/api/search') {
        const query = requestUrl.searchParams.get('q') || ''
        const requestedPage = Number(requestUrl.searchParams.get('page') || 1)
        const requestedPageSize = Number(requestUrl.searchParams.get('page_size') || requestUrl.searchParams.get('limit') || 20)
        const page = Number.isInteger(requestedPage) && requestedPage > 0 ? requestedPage : 1
        const pageSize = Number.isInteger(requestedPageSize) && requestedPageSize > 0 ? Math.min(requestedPageSize, 100) : 20
        if (!query.trim()) return json(res, 200, { query, page, page_size: pageSize, total: 0, total_pages: 0, results: [] })
        const { results, total } = await catalog.searchCatalogPage(query, pageSize, (page - 1) * pageSize)
        return json(res, 200, {
          query,
          page,
          page_size: pageSize,
          total,
          total_pages: Math.ceil(total / pageSize),
          results
        })
      }
      return serveStatic(res, requestUrl.pathname)
    } catch (error) {
      json(res, 500, { error: error.message })
    }
  })
}

let parsed
try {
  parsed = options(process.argv.slice(2))
} catch (error) {
  process.stderr.write(`error: ${error.message}\n`)
  process.exitCode = 2
}

if (parsed) {
  const catalog = await openStorage({ db: parsed.db })
  const server = createServer(catalog)
  server.listen(parsed.port, parsed.host, () => {
    const address = server.address()
    process.stdout.write(`DHT Search Console: http://${parsed.host}:${address.port}\n`)
  })
  const close = () => server.close(() => void catalog.close())
  process.once('SIGINT', close)
  process.once('SIGTERM', close)
}
