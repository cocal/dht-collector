#!/usr/bin/env bash
set -u -o pipefail

LOCK_FILE=/run/lock/dht-self-check.lock
COOLDOWN_SECONDS=300

exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

log() { logger -t dht-self-check -- "$*"; printf '%s\n' "$*"; }

restart_unit() {
  local unit="$1"
  local now last=0 cooldown_file
  cooldown_file="/run/dht-self-check.last-restart.${unit//[^A-Za-z0-9_.-]/_}"
  now=$(date +%s)
  if [[ -f "$cooldown_file" ]]; then
    read -r last < "$cooldown_file" || last=0
  fi
  if (( now - last < COOLDOWN_SECONDS )); then
    log "restart suppressed by cooldown: $unit"
    return 0
  fi
  log "restarting unhealthy unit: $unit"
  if systemctl restart "$unit"; then
    printf '%s\n' "$now" > "$cooldown_file"
  else
    log "restart failed: $unit"
  fi
}

ensure_active() {
  local unit="$1"
  if ! systemctl is-active --quiet "$unit"; then
    restart_unit "$unit"
  fi
}

# Check the datastore first so dependent bridge/dashboard units recover in order.
if ! systemctl is-active --quiet keydb.service || ! timeout 3s keydb-cli ping 2>/dev/null | grep -qx PONG; then
  restart_unit keydb.service
fi
ensure_active dht-passive-collector.service
ensure_active dht-peer-explorer.service
ensure_active dht-monitor-ingest.service
ensure_active dht-redis-monitor.service

if ! systemctl is-active --quiet dht-monitor-redis-bridge.service; then
  restart_unit dht-monitor-redis-bridge.service
fi

if ! systemctl is-active --quiet dht-search-dashboard.service \
  || ! curl -fsS --max-time 5 http://127.0.0.1:4173/api/system 2>/dev/null \
    | grep -q '"available":true'; then
  restart_unit dht-search-dashboard.service
fi

log "health check completed"
