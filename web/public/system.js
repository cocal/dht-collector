const $ = (id) => document.getElementById(id)
const bytes = (value) => { const n = Number(value || 0); if (n < 1024) return `${n} B`; if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`; if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`; return `${(n / 1024 ** 3).toFixed(2)} GB` }
const duration = (value) => { const n = Number(value || 0); const d = Math.floor(n / 86400); const h = Math.floor(n % 86400 / 3600); const m = Math.floor(n % 3600 / 60); return `${d}d ${h}h ${m}m` }
async function refresh() {
  try {
    const response = await fetch('api/system', { cache: 'no-store' }); if (!response.ok) throw new Error(response.status)
    const data = await response.json(); const services = data.services || {}; const redis = data.redis || {}; const host = data.host || {}
    const nodes = [...document.querySelectorAll('.arch-node')]; nodes.forEach((node) => { const ok = services[node.dataset.service] === 'active'; node.classList.toggle('online', ok); node.classList.toggle('offline', !ok); node.querySelector('em').textContent = ok ? '运行中' : (services[node.dataset.service] || '不可用') })
    document.querySelectorAll('.arch-link').forEach((link) => link.classList.toggle('live', services[link.dataset.from] === 'active' && services[link.dataset.to] === 'active'))
    const allGood = nodes.every((node) => node.classList.contains('online')); $('overall-state').innerHTML = `<i></i>${allGood ? '链路正常' : '存在异常'}`; $('overall-state').className = `status-badge ${allGood ? 'good' : 'bad'}`
    $('redis-status').textContent = redis.available ? '正常' : '不可用'; $('redis-status').className = redis.available ? 'good' : 'bad'; $('redis-version').textContent = redis.version || '—'; $('redis-clients').textContent = redis.connected_clients ?? '—'; $('redis-keys').textContent = redis.keys ?? '—'; $('redis-fields').textContent = redis.summary_fields ?? '—'; $('redis-stream').textContent = redis.event_stream_length ?? '—'; $('redis-memory').textContent = redis.available ? `${bytes(redis.used_memory_bytes)} / 峰值 ${bytes(redis.peak_memory_bytes)}` : '—'; $('redis-uptime').textContent = redis.available ? duration(redis.uptime_seconds) : '—'
    const memory = host.memory || {}; const disk = host.disk || {}; $('host-memory').textContent = memory.memavailable ? `${bytes(memory.memavailable)} 可用 / ${bytes(memory.memtotal)} 总计` : '—'; $('host-swap').textContent = memory.swaptotal ? `${bytes(memory.swaptotal - memory.swapfree)} 已用 / ${bytes(memory.swaptotal)} 总计` : '—'; $('host-disk').textContent = disk.total ? `${bytes(disk.total - disk.free)} 已用 / ${bytes(disk.total)} 总计` : '—'; $('updated-at').textContent = new Date().toLocaleTimeString('zh-CN'); $('connection-state').innerHTML = '<i></i>已连接'; $('connection-state').className = 'connection-state online'
  } catch (_) { $('connection-state').innerHTML = '<i></i>API 离线'; $('connection-state').className = 'connection-state offline' }
}
refresh(); setInterval(refresh, 5000)
