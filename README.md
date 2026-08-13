# DHT Collector 0.1

This is the first runnable slice of DHT Search v2. It uses
[`bittorrent-dht`](https://github.com/webtorrent/bittorrent-dht) to perform
Mainline DHT discovery and emits versioned JSONL events. It deliberately does
not download torrent payloads, announce a peer, write to the DHT, or expose a
public search API.

## Run

```bash
npm install
npm run check

# Confirm UDP bind and bootstrap behavior for five seconds.
npm start -- health --duration-ms 5000

# Look up a known v1 infohash. The result is JSONL on stdout.
npm start -- lookup <40-character-hex-infohash> --timeout-ms 30000

# Persist collector events and feed them into the metadata worker.
node src/index.js lookup <40-character-hex-infohash> --event-log ./var/events.jsonl
node src/metadata-fetcher.js --input ./var/events.jsonl --output ./var/metadata-events.jsonl

# Write collector and metadata events directly to a local SQLite database.
node src/index.js health --duration-ms 5000 --db ./var/dht-search.db
node src/metadata-fetcher.js --input ./var/events.jsonl --db ./var/dht-search.db

# Ingest normalized manifests into the local development catalog and search it.
node src/catalog.js ingest --input ./var/metadata-events.jsonl --db ./var/catalog.db
node src/catalog.js search --query "linux image" --db ./var/catalog.db

# Start the local dashboard (http://127.0.0.1:4173).
npm run dashboard -- --db ./var/dht-search.db --port 4173
```

## Passive DHT Discovery

The host deployment runs a passive DHT node. It receives incoming `get_peers`
and `announce_peer` requests from other DHT participants, extracts the v1
Infohash, and performs one bounded lookup/metadata attempt for each newly
discovered resource. A live `announce_peer` for an already-known Infohash is
also queued ahead of lookups, because it includes a current peer endpoint; at
most `--max-peers-per-infohash` distinct announced endpoints are attempted per
Infohash. Infohashes are persisted with a unique key, so the same
resource is not registered twice.

Each observation refreshes `last_seen_at`. Resources not observed for seven
days are marked `invalid` by an hourly sweep; they are retained for history and
return to `active` automatically when observed again.

Only resources observed in the last 24 hours are loaded into the collector's
in-memory deduplication cache. An hourly task removes expired cache entries;
older cache misses are checked and reactivated atomically in the database.

The dashboard's `嗅探服务` switch starts and stops this service:

```bash
systemctl status dht-passive-collector.service
journalctl -u dht-passive-collector.service -f
```

The collector can be bounded with `--max-resources`, `--max-concurrent`, and
`--max-peers-per-infohash`; `--max-resources 0` means unlimited resources. It
never downloads torrent payloads.

Metadata work is stored in PostgreSQL in production before execution. Jobs survive collector
restarts, use exponential retry backoff after misses, and prioritize live
`announce_peer` endpoints. The in-memory queue remains bounded by
`--max-pending`, while `--dht-concurrency` controls outbound UDP lookup
parallelism. High-volume incoming queries are stored as ten-second aggregates;
unique resource discoveries and metadata outcomes remain individual events.
`--dht-nodes` starts independent DHT identities on sequential UDP ports to
cover more of the keyspace from one host. Open the full configured port range
in the host and provider firewalls.

## Authorized Discovery And Indexing

For an explicitly authorized release, the exact indexer remains available as a
separate tool. Add a v1 infohash and an auditable reference to a local JSONL
file:

```json
{"info_hash":"0123456789abcdef0123456789abcdef01234567","authorization_ref":"publisher-release-record"}
```

Run the approved-indexer to perform an exact DHT lookup. It requests metadata
only from up to three discovered Peers by default, verifies the v1 infohash,
and writes all discovery, metadata, and catalog events to the same database:

```bash
npm run approved-index -- \
  --input ./var/approved-infohashes.jsonl \
  --db ./var/dht-search.db \
  --port 51413
```

The dashboard switch controls only `dht-passive-collector.service`. Its state
is also available through `GET /api/sniffer`; `POST /api/sniffer` accepts only
`{ "enabled": true }` or `{ "enabled": false }`.

Some DHT peer announcements are stale or do not provide metadata. An authorized
publisher `.torrent` file can be imported as a separately labeled, verified
metadata source; it never downloads payload data:

```bash
npm run import-torrent -- \
  --input ./release.torrent \
  --authorization-ref publisher-release-record \
  --db ./var/dht-search.db
```

The CLI binds an ephemeral UDP port by default. Use `--port` only when a
stable port is required. Use `--no-peer-address` when output must omit peer
endpoint details.

## Java collector

The passive collector also has a Java 21 implementation under `java/`. It uses
mldht for the DHT protocol and routing table, virtual threads for observation
work, and HikariCP for PostgreSQL pooling:

```bash
cd java
mvn test
mvn package
java -jar target/dht-collector-java-0.1.0.jar --port 51413 --dht-nodes 12
```

The Java service uses the same `discovered_resource` and `probe_event` tables,
so it can be introduced without a database migration. It covers passive DHT
routing and resource observation. BEP9 metadata fetching and the authorized
indexer remain in the existing Node.js tools until their Java worker is
migrated separately. A generic unit template is in
`deploy/dht-passive-collector-java.service`; adjust paths and the private
environment-file location for the target host. Keep the current Node service
running when metadata jobs must continue to execute.

## Event contract

Events include `event_id`, `schema_version`, `event`, and `occurred_at` so they
can be sent directly to the event log described in
[`DHT_SEARCH_V2_PLAN.md`](../DHT_SEARCH_V2_PLAN.md). The main events are:

- `collector.ready`
- `passive.collector_ready`
- `dht.resource_discovered`
- `dht.warning`
- `dht.peer_discovered`
- `dht.lookup_completed`
- `metadata.worker_ready`
- `metadata.fetch_started`
- `metadata.fetch_completed`
- `metadata.fetch_failed`
- `collector.snapshot`
- `collector.failed`

## Current boundary

The collector only requests BEP 9 metadata and verifies the v1 infohash; it
never requests piece data. Metadata text prefers the standard UTF-8 fields and
uses charset detection for legacy names and paths. v2/hybrid identifier support
belongs in the event schema before metadata indexing is enabled.

The dashboard trend endpoint is included in `GET /api/dashboard`. It returns
five one-minute buckets for unique resource discoveries, incoming DHT queries,
actual operation failures, and protocol warning occurrences. A warning such as
`Missing delimiter ":"` is a malformed UDP bencode packet reported by the DHT
decoder; it is aggregated separately and does not mean the collector failed to
join the network.

`catalog.js` uses Node 22's built-in SQLite and FTS5 as the local development
and rollback adapter. Production uses PostgreSQL with batched event writes,
pre-aggregated minute metrics, persistent counters, and concurrent job claims
using `FOR UPDATE SKIP LOCKED`.

Both production services should load a private environment file (for example,
`/etc/dht-search/db.env`, kept outside this repository). PostgreSQL is selected
when `DATABASE_URL` is present; otherwise `--db` selects SQLite. TLS
uses `PGSSLMODE=verify-full` and `PGSSLROOTCERT`. Keep the application pools
below the PostgreSQL role limit; the deployed collector uses `PGPOOL_MAX=5`
and the dashboard uses `PGPOOL_MAX=2`.

Migrate a live SQLite database with an online snapshot followed by a stopped
delta. The delta is idempotent and reconciles content keys before verification:

```bash
set -a
source /etc/dht-search/db.env
set +a
npm run migrate:postgres -- --db ./var/dht-search.db --mode full
systemctl stop dht-passive-collector.service dht-search-dashboard.service
npm run migrate:postgres -- --db ./var/dht-search.db --mode delta
npm run migrate:postgres -- --db ./var/dht-search.db --mode verify
```

Keep the SQLite file after cutover. To roll back, remove the `EnvironmentFile`
line from both systemd units, run `systemctl daemon-reload`, and restart them;
the existing `--db` arguments then select SQLite.

`web/server.js` exposes dashboard endpoints
(`GET /api/dashboard`, paginated `GET /api/content` and `GET /api/search`,
`GET /api/health`, and the fixed service control endpoint `/api/sniffer`) and
serves the single-page console from `web/public`. Pagination uses `page` and
`page_size` query parameters and returns `total` plus `total_pages`.
