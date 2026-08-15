const SEARCH_PAGE_SIZE = 20
const state = { data: null, filter: 'all', catalogResults: null, catalog: null, searchResults: null, search: null, pageLoading: false }
let snifferBusy = false

const $ = (selector) => document.querySelector(selector)
const apiUrl = (path) => new URL(path, window.location.href).toString()
const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]))
const shortHash = (value) => value ? `${value.slice(0, 8)}…${value.slice(-6)}` : '—'
const formatNumber = (value) => new Intl.NumberFormat('zh-CN').format(Number(value || 0))
const formatBytes = (value) => {
  const bytes = Number(value || 0)
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`
  return `${(bytes / 1024 ** 3).toFixed(1)} GB`
}
const formatTime = (value) => value ? new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date(value)) : '—'
const eventLabel = (type) => ({
  'collector.ready': '采集就绪',
  'collector.snapshot': '系统快照',
  'collector.failed': '采集失败',
  'dht.peer_discovered': 'Peer 发现',
  'dht.resource_discovered': '资源发现',
  'dht.query_received': 'DHT 查询',
  'dht.lookup_completed': '查询完成',
  'dht.lookup_started': '被动查询',
  'dht.lookup_failed': '查询失败',
  'passive.collector_ready': '被动采集就绪',
  'passive.resource_limit_reached': '达到资源配额',
  'dht.warning': 'DHT 警告',
  'dht.error': 'DHT 错误',
  'metadata.worker_ready': 'Metadata 就绪',
  'metadata.fetch_started': 'Metadata 请求',
  'metadata.fetch_completed': 'Metadata 完成',
  'metadata.fetch_failed': 'Metadata 失败',
  'metadata.import_completed': '授权元数据导入'
}[type] || type)
const eventTone = (type) => type.includes('failed') || type.includes('error') ? 'danger' : type.includes('warning') ? 'warning' : type.includes('peer') || type.includes('completed') ? 'success' : 'neutral'

function renderSummary(summary) {
  $('#metric-probes').textContent = formatNumber(summary.probes)
  $('#metric-peers').textContent = formatNumber(summary.peers)
  $('#metric-lookups').textContent = formatNumber(summary.lookups)
  $('#metric-content').textContent = formatNumber(summary.content)
  $('#metric-files').textContent = `${formatNumber(summary.files)} 个文件条目`
  $('#last-event').textContent = formatTime(summary.last_event_at)
  if (!state.search) $('#content-count').textContent = formatNumber(summary.content)
  $('#status-collector').textContent = summary.discovered ? `${formatNumber(summary.discovered)} 个 Infohash 已发现` : '等待 DHT 请求'
  $('#status-metadata').textContent = summary.content ? '内容已入库' : '等待结果'
}

function renderTrend(trend) {
  const canvas = $('#trend-chart')
  const empty = $('#trend-empty')
  const buckets = Array.isArray(trend?.buckets) ? trend.buckets : []
  if (!buckets.length) {
    empty.hidden = false
    return
  }
  empty.hidden = true
  const rect = canvas.getBoundingClientRect()
  const width = Math.max(280, Math.round(rect.width || canvas.parentElement.clientWidth || 280))
  const height = Math.max(180, Math.round(rect.height || 240))
  const ratio = Math.min(window.devicePixelRatio || 1, 2)
  canvas.width = width * ratio
  canvas.height = height * ratio
  const context = canvas.getContext('2d')
  context.setTransform(ratio, 0, 0, ratio, 0, 0)
  context.clearRect(0, 0, width, height)

  const styles = getComputedStyle(document.documentElement)
  const border = styles.getPropertyValue('--border').trim() || '#39423a'
  const muted = styles.getPropertyValue('--muted').trim() || '#9da99e'
  const lime = styles.getPropertyValue('--lime').trim() || '#b8ef65'
  const teal = styles.getPropertyValue('--teal').trim() || '#65d5ca'
  const coral = styles.getPropertyValue('--coral').trim() || '#f08d7e'
  const amber = styles.getPropertyValue('--amber').trim() || '#f4c46a'
  const padding = { top: 14, right: 16, bottom: 31, left: 38 }
  const plotWidth = width - padding.left - padding.right
  const plotHeight = height - padding.top - padding.bottom
  const maxValue = Math.max(1, ...buckets.flatMap((bucket) => [bucket.links, bucket.queries, bucket.failures, bucket.indexed]))
  const y = (value) => padding.top + plotHeight - ((value / maxValue) * plotHeight)
  const x = (index) => buckets.length === 1
    ? padding.left + (plotWidth / 2)
    : padding.left + ((index / (buckets.length - 1)) * plotWidth)

  context.font = '11px "Fira Code", monospace'
  context.lineWidth = 1
  context.strokeStyle = border
  context.fillStyle = muted
  context.textAlign = 'right'
  for (let step = 0; step <= 3; step++) {
    const value = Math.round((maxValue * step) / 3)
    const lineY = y(value)
    context.beginPath()
    context.moveTo(padding.left, lineY)
    context.lineTo(width - padding.right, lineY)
    context.stroke()
    context.fillText(formatNumber(value), padding.left - 8, lineY + 4)
  }
  context.textAlign = 'center'
  const timeFormatter = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })
  buckets.forEach((bucket, index) => {
    context.fillText(timeFormatter.format(new Date(bucket.at)), x(index), height - 10)
  })

  const drawSeries = (key, color) => {
    context.strokeStyle = color
    context.lineWidth = 2
    context.lineJoin = 'round'
    context.lineCap = 'round'
    context.beginPath()
    buckets.forEach((bucket, index) => {
      const pointX = x(index)
      const pointY = y(Number(bucket[key] || 0))
      if (index === 0) context.moveTo(pointX, pointY)
      else context.lineTo(pointX, pointY)
    })
    context.stroke()
    context.fillStyle = color
    buckets.forEach((bucket, index) => {
      context.beginPath()
      context.arc(x(index), y(Number(bucket[key] || 0)), 2.5, 0, Math.PI * 2)
      context.fill()
    })
  }
  drawSeries('links', lime)
  drawSeries('queries', teal)
  drawSeries('failures', coral)
  drawSeries('indexed', amber)
  const warnings = buckets.reduce((sum, bucket) => sum + Number(bucket.warnings || 0), 0)
  const links = buckets.reduce((sum, bucket) => sum + Number(bucket.links || 0), 0)
  const queries = buckets.reduce((sum, bucket) => sum + Number(bucket.queries || 0), 0)
  const failures = buckets.reduce((sum, bucket) => sum + Number(bucket.failures || 0), 0)
  const indexed = buckets.reduce((sum, bucket) => sum + Number(bucket.indexed || 0), 0)
  $('#trend-note').textContent = `协议告警 ${formatNumber(warnings)} 次 · 资源发现 ${formatNumber(links)} · 查询 ${formatNumber(queries)} · 失败 ${formatNumber(failures)} · 新增索引 ${formatNumber(indexed)}`
  canvas.setAttribute('aria-label', `近五分钟趋势：资源发现 ${formatNumber(links)}，查询 ${formatNumber(queries)}，失败 ${formatNumber(failures)}，新增索引 ${formatNumber(indexed)}`)
}

function renderResourceTrend(trend) {
  const canvas = $('#resource-trend-chart')
  const empty = $('#resource-trend-empty')
  const buckets = Array.isArray(trend?.buckets) ? trend.buckets : []
  if (!buckets.length) {
    empty.hidden = false
    $('#resource-trend-total').textContent = '近 5 分钟 · 0'
    $('#resource-trend-note').textContent = '峰值 0 个/分钟'
    $('#resource-trend-table').innerHTML = ''
    return
  }
  empty.hidden = true
  const rect = canvas.getBoundingClientRect()
  const width = Math.max(280, Math.round(rect.width || canvas.parentElement.clientWidth || 280))
  const height = Math.max(180, Math.round(rect.height || 240))
  const ratio = Math.min(window.devicePixelRatio || 1, 2)
  canvas.width = width * ratio
  canvas.height = height * ratio
  const context = canvas.getContext('2d')
  context.setTransform(ratio, 0, 0, ratio, 0, 0)
  context.clearRect(0, 0, width, height)

  const styles = getComputedStyle(document.documentElement)
  const border = styles.getPropertyValue('--border').trim() || '#39423a'
  const muted = styles.getPropertyValue('--muted').trim() || '#9da99e'
  const lime = styles.getPropertyValue('--lime').trim() || '#b8ef65'
  const padding = { top: 30, right: 16, bottom: 31, left: 38 }
  const plotWidth = width - padding.left - padding.right
  const plotHeight = height - padding.top - padding.bottom
  const values = buckets.map((bucket) => Number(bucket.links || 0))
  const maxValue = Math.max(1, ...values)
  const y = (value) => padding.top + plotHeight - ((value / maxValue) * plotHeight)
  const x = (index) => buckets.length === 1
    ? padding.left + (plotWidth / 2)
    : padding.left + ((index / (buckets.length - 1)) * plotWidth)

  context.font = '11px "Fira Code", monospace'
  context.lineWidth = 1
  context.strokeStyle = border
  context.fillStyle = muted
  context.textAlign = 'right'
  for (let step = 0; step <= 3; step++) {
    const value = Math.round((maxValue * step) / 3)
    const lineY = y(value)
    context.beginPath()
    context.moveTo(padding.left, lineY)
    context.lineTo(width - padding.right, lineY)
    context.stroke()
    context.fillText(formatNumber(value), padding.left - 8, lineY + 4)
  }

  const timeFormatter = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })
  context.textAlign = 'center'
  buckets.forEach((bucket, index) => {
    context.fillText(timeFormatter.format(new Date(bucket.at)), x(index), height - 10)
  })

  context.beginPath()
  context.moveTo(x(0), padding.top + plotHeight)
  values.forEach((value, index) => context.lineTo(x(index), y(value)))
  context.lineTo(x(values.length - 1), padding.top + plotHeight)
  context.closePath()
  context.fillStyle = lime
  context.globalAlpha = 0.12
  context.fill()
  context.globalAlpha = 1

  context.beginPath()
  values.forEach((value, index) => {
    if (index === 0) context.moveTo(x(index), y(value))
    else context.lineTo(x(index), y(value))
  })
  context.strokeStyle = lime
  context.lineWidth = 2.5
  context.lineJoin = 'round'
  context.lineCap = 'round'
  context.stroke()

  values.forEach((value, index) => {
    const pointX = x(index)
    const pointY = y(value)
    context.beginPath()
    context.arc(pointX, pointY, 4, 0, Math.PI * 2)
    context.fillStyle = lime
    context.fill()
    context.fillStyle = lime
    context.font = '600 10px "Fira Code", monospace'
    context.fillText(formatNumber(value), pointX, Math.max(12, pointY - 9))
  })

  const total = values.reduce((sum, value) => sum + value, 0)
  const peak = Math.max(0, ...values)
  $('#resource-trend-total').textContent = `近 5 分钟 · ${formatNumber(total)}`
  $('#resource-trend-note').textContent = `峰值 ${formatNumber(peak)} 个/分钟 · 去重 Infohash ${formatNumber(total)} 个`
  $('#resource-trend-table').innerHTML = buckets.map((bucket, index) => `<tr><td>${escapeHtml(timeFormatter.format(new Date(bucket.at)))}</td><td>${formatNumber(values[index])}</td></tr>`).join('')
  canvas.setAttribute('aria-label', `近五分钟资源嗅探趋势：共发现 ${formatNumber(total)} 个 Infohash，峰值 ${formatNumber(peak)} 个每分钟`)
}

function renderIndexedTrend(trend) {
  const canvas = $('#indexed-trend-chart')
  const empty = $('#indexed-trend-empty')
  const buckets = Array.isArray(trend?.buckets) ? trend.buckets : []
  if (!buckets.length) {
    empty.hidden = false
    $('#indexed-trend-total').textContent = '近 5 分钟 · 0'
    $('#indexed-trend-note').textContent = '峰值 0 个/分钟'
    $('#indexed-trend-table').innerHTML = ''
    return
  }
  empty.hidden = true
  const rect = canvas.getBoundingClientRect()
  const width = Math.max(280, Math.round(rect.width || canvas.parentElement.clientWidth || 280))
  const height = Math.max(180, Math.round(rect.height || 240))
  const ratio = Math.min(window.devicePixelRatio || 1, 2)
  canvas.width = width * ratio
  canvas.height = height * ratio
  const context = canvas.getContext('2d')
  context.setTransform(ratio, 0, 0, ratio, 0, 0)
  context.clearRect(0, 0, width, height)

  const styles = getComputedStyle(document.documentElement)
  const border = styles.getPropertyValue('--border').trim() || '#39423a'
  const muted = styles.getPropertyValue('--muted').trim() || '#9da99e'
  const amber = styles.getPropertyValue('--amber').trim() || '#f4c46a'
  const padding = { top: 14, right: 16, bottom: 31, left: 38 }
  const plotWidth = width - padding.left - padding.right
  const plotHeight = height - padding.top - padding.bottom
  const values = buckets.map((bucket) => Number(bucket.indexed || 0))
  const maxValue = Math.max(1, ...values)
  const y = (value) => padding.top + plotHeight - ((value / maxValue) * plotHeight)
  const x = (index) => buckets.length === 1
    ? padding.left + (plotWidth / 2)
    : padding.left + ((index / (buckets.length - 1)) * plotWidth)

  context.font = '11px "Fira Code", monospace'
  context.lineWidth = 1
  context.strokeStyle = border
  context.fillStyle = muted
  context.textAlign = 'right'
  for (let step = 0; step <= 3; step++) {
    const value = Math.round((maxValue * step) / 3)
    const lineY = y(value)
    context.beginPath()
    context.moveTo(padding.left, lineY)
    context.lineTo(width - padding.right, lineY)
    context.stroke()
    context.fillText(formatNumber(value), padding.left - 8, lineY + 4)
  }

  const timeFormatter = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })
  context.textAlign = 'center'
  buckets.forEach((bucket, index) => context.fillText(timeFormatter.format(new Date(bucket.at)), x(index), height - 10))
  context.beginPath()
  values.forEach((value, index) => index === 0 ? context.moveTo(x(index), y(value)) : context.lineTo(x(index), y(value)))
  context.strokeStyle = amber
  context.lineWidth = 2.5
  context.lineJoin = 'round'
  context.lineCap = 'round'
  context.stroke()
  values.forEach((value, index) => {
    context.beginPath()
    context.arc(x(index), y(value), 4, 0, Math.PI * 2)
    context.fillStyle = amber
    context.fill()
    context.font = '600 10px "Fira Code", monospace'
    context.fillText(formatNumber(value), x(index), Math.max(12, y(value) - 9))
  })

  const total = values.reduce((sum, value) => sum + value, 0)
  const peak = Math.max(0, ...values)
  $('#indexed-trend-total').textContent = `近 5 分钟 · ${formatNumber(total)}`
  $('#indexed-trend-note').textContent = `峰值 ${formatNumber(peak)} 个/分钟 · 数据来自 content.indexed`
  $('#indexed-trend-table').innerHTML = buckets.map((bucket, index) => `<tr><td>${escapeHtml(timeFormatter.format(new Date(bucket.at)))}</td><td>${formatNumber(values[index])}</td></tr>`).join('')
  canvas.setAttribute('aria-label', `近五分钟新增索引趋势：共 ${formatNumber(total)} 个内容，峰值 ${formatNumber(peak)} 个每分钟`)
}

function renderEvents(probes) {
  const rows = state.filter === 'all' ? probes : probes.filter((probe) => probe.event_type === state.filter)
  $('#event-count').textContent = `${formatNumber(rows.length)} 条`
  $('#event-empty').hidden = rows.length > 0
  $('#event-table').innerHTML = rows.map((probe) => {
    const peer = probe.peer_host ? `${escapeHtml(probe.peer_host)}:${escapeHtml(probe.peer_port)}` : '—'
    const detail = probe.message || (probe.info_hash ? `hash ${shortHash(probe.info_hash)}` : probe.mode || '—')
    return `<tr>
      <td data-label="事件"><span class="event-pill ${eventTone(probe.event_type)}"><i></i>${escapeHtml(eventLabel(probe.event_type))}</span></td>
      <td data-label="时间" class="time-cell">${escapeHtml(formatTime(probe.occurred_at))}</td>
      <td data-label="Infohash" class="mono">${probe.info_hash ? `<code title="${escapeHtml(probe.info_hash)}">${escapeHtml(shortHash(probe.info_hash))}</code>` : '—'}</td>
      <td data-label="Peer" class="mono">${peer}</td>
      <td data-label="详情" class="detail-cell">${escapeHtml(detail)}</td>
    </tr>`
  }).join('')
}

function renderContent(items) {
  const rows = state.searchResults ?? state.catalogResults ?? items
  if (!rows.length) {
    $('#content-results').innerHTML = `<div class="empty-state">${state.searchResults !== null ? '没有匹配内容' : '暂无已验证内容'}</div>`
    renderPagination()
    return
  }
  $('#content-results').innerHTML = rows.map((item) => `<div class="content-row">
    <div class="content-row-main"><strong>${escapeHtml(item.name)}</strong><span class="mono">${escapeHtml(shortHash(item.info_hash))}</span></div>
    <div class="content-row-meta"><span>${escapeHtml(item.variant)}</span><span>${formatBytes(item.total_size)}</span><span>${formatNumber(item.file_count)} files</span><span>入库 ${escapeHtml(formatTime(item.created_at))}</span></div>
  </div>`).join('')
  renderPagination()
}

function renderPagination() {
  const pagination = $('#search-pagination')
  const pageState = state.search || state.catalog
  pagination.hidden = !pageState || pageState.total === 0
  if (!pageState) return
  $('#search-page-status').textContent = `第 ${formatNumber(pageState.page)} / ${formatNumber(pageState.totalPages)} 页 · 共 ${formatNumber(pageState.total)} 条`
  $('#search-prev').disabled = state.pageLoading || pageState.page <= 1
  $('#search-next').disabled = state.pageLoading || pageState.page >= pageState.totalPages
  $('#content-count').textContent = formatNumber(pageState.total)
}

async function loadDashboard() {
  try {
    const catalogPage = state.catalog?.page || 1
    const [response, snifferResponse, catalogResponse] = await Promise.all([
      fetch(apiUrl('api/dashboard?limit=100'), { cache: 'no-store' }),
      fetch(apiUrl('api/sniffer'), { cache: 'no-store' }),
      state.search ? Promise.resolve(null) : fetch(apiUrl(`api/content?page=${catalogPage}&page_size=${SEARCH_PAGE_SIZE}`), { cache: 'no-store' })
    ])
    if (!response.ok) throw new Error(`API ${response.status}`)
    if (!snifferResponse.ok) throw new Error(`Sniffer API ${snifferResponse.status}`)
    state.data = await response.json()
    if (catalogResponse) {
      if (!catalogResponse.ok) throw new Error(`Content API ${catalogResponse.status}`)
      const catalogData = await catalogResponse.json()
      state.catalogResults = catalogData.results
      state.catalog = {
        page: catalogData.page,
        total: catalogData.total,
        totalPages: catalogData.total_pages
      }
    }
    renderSniffer(await snifferResponse.json())
    renderSummary(state.data.summary)
    renderTrend(state.data.trend)
    renderResourceTrend(state.data.trend)
    renderIndexedTrend(state.data.trend)
    renderEvents(state.data.probes)
    renderContent(state.data.content)
    $('#connection-state').innerHTML = '<i></i> 已连接'
    $('#connection-state').className = 'connection-state online'
  } catch (error) {
    $('#connection-state').innerHTML = `<i></i> API 离线`
    $('#connection-state').className = 'connection-state offline'
  }
}

function renderSniffer(data) {
  const toggle = $('#sniffer-toggle')
  const label = $('#sniffer-state')
  const active = data.status === 'active'
  toggle.checked = active
  toggle.disabled = snifferBusy
  label.textContent = snifferBusy ? '切换中' : active ? '运行中' : data.status === 'failed' ? '失败' : '已停止'
  label.className = active ? 'sniffer-on' : 'sniffer-off'
}

async function toggleSniffer(event) {
  if (snifferBusy) return
  const toggle = event.currentTarget
  const desired = toggle.checked
  snifferBusy = true
  renderSniffer({ status: desired ? 'activating' : 'deactivating' })
  try {
    const response = await fetch(apiUrl('api/sniffer'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enabled: desired })
    })
    const data = await response.json()
    if (!response.ok) throw new Error(data.error || `API ${response.status}`)
    renderSniffer(data)
  } catch (error) {
    toggle.checked = !desired
    renderSniffer({ status: desired ? 'failed' : 'active' })
    $('#connection-state').innerHTML = `<i></i> ${escapeHtml(error.message)}`
    $('#connection-state').className = 'connection-state offline'
  } finally {
    snifferBusy = false
    if (state.data) renderSniffer(await fetch(apiUrl('api/sniffer'), { cache: 'no-store' }).then((response) => response.json()).catch(() => ({ status: toggle.checked ? 'active' : 'unknown' })))
  }
}

async function loadCatalogPage(page) {
  state.pageLoading = true
  renderPagination()
  try {
    const response = await fetch(apiUrl(`api/content?page=${page}&page_size=${SEARCH_PAGE_SIZE}`), { cache: 'no-store' })
    const data = await response.json()
    if (!response.ok) throw new Error(data.error || `API ${response.status}`)
    state.catalogResults = data.results
    state.catalog = { page: data.page, total: data.total, totalPages: data.total_pages }
  } finally {
    state.pageLoading = false
    renderContent(state.data?.content || [])
  }
}

async function loadSearchPage(page, requestedQuery) {
  const query = (requestedQuery ?? $('#search-input').value).trim()
  if (!query) {
    state.searchResults = null
    state.search = null
    state.pageLoading = false
    await loadCatalogPage(1)
    return
  }
  state.pageLoading = true
  renderPagination()
  try {
    const response = await fetch(apiUrl(`api/search?q=${encodeURIComponent(query)}&page=${page}&page_size=${SEARCH_PAGE_SIZE}`), { cache: 'no-store' })
    const data = await response.json()
    if (!response.ok) throw new Error(data.error || `API ${response.status}`)
    state.searchResults = data.results
    state.search = {
      query: data.query,
      page: data.page,
      total: data.total,
      totalPages: data.total_pages
    }
  } catch (error) {
    state.searchResults = []
    state.search = { query, page, total: 0, totalPages: 0 }
    $('#connection-state').innerHTML = `<i></i> ${escapeHtml(error.message)}`
    $('#connection-state').className = 'connection-state offline'
  } finally {
    state.pageLoading = false
    renderContent(state.data?.content || [])
  }
}

async function search(event) {
  event.preventDefault()
  await loadSearchPage(1)
}

function changeContentPage(delta) {
  const pageState = state.search || state.catalog
  if (!pageState || state.pageLoading) return
  const page = Math.min(pageState.totalPages, Math.max(1, pageState.page + delta))
  if (state.search) loadSearchPage(page, state.search.query)
  else loadCatalogPage(page)
}

function connectLiveSummary() {
  if (!window.EventSource) return
  const stream = new EventSource(apiUrl('api/stream'))
  stream.onmessage = (message) => {
    try {
      const payload = JSON.parse(message.data)
      if (!state.data || !payload.summary) return
      state.data.summary = payload.summary
      renderSummary(payload.summary)
      $('#connection-state').innerHTML = '<i></i> 实时连接'
      $('#connection-state').className = 'connection-state online'
    } catch (_) { /* Ignore malformed optional monitor events. */ }
  }
  stream.onerror = () => {
    stream.close()
    setTimeout(connectLiveSummary, 5000)
  }
}

$('#refresh-button').addEventListener('click', loadDashboard)
$('#sniffer-toggle').addEventListener('change', toggleSniffer)
$('#event-filter').addEventListener('change', (event) => { state.filter = event.target.value; renderEvents(state.data?.probes || []) })
$('#search-form').addEventListener('submit', search)
$('#search-prev').addEventListener('click', () => changeContentPage(-1))
$('#search-next').addEventListener('click', () => changeContentPage(1))
let resizeTimer
window.addEventListener('resize', () => {
  clearTimeout(resizeTimer)
  resizeTimer = setTimeout(() => {
    renderTrend(state.data?.trend)
    renderResourceTrend(state.data?.trend)
    renderIndexedTrend(state.data?.trend)
  }, 100)
})
loadDashboard()
connectLiveSummary()
setInterval(loadDashboard, 30000)
