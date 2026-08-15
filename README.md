# DHT Collector 0.1

The production runtime is Java 21. One shaded jar provides passive Mainline
DHT collection, BEP 9 metadata retrieval, PostgreSQL catalog access, the
dashboard API/static UI, authorized indexing, JSONL catalog tools, and SQLite
to PostgreSQL migration. It never downloads torrent payload pieces or writes
to the DHT.

## Run

```bash
cd java
mvn test
mvn package
export DATABASE_URL='postgresql://user:password@host/db' # keep secrets outside git

# Confirm UDP bind and bootstrap behavior for five seconds.
java -jar target/dht-collector-java-0.1.0.jar --mode health --duration-ms 5000

# Look up a known v1 infohash and emit versioned JSONL events.
java -jar target/dht-collector-java-0.1.0.jar --mode lookup \
  --info-hash <40-character-hex-infohash> --timeout-ms 30000

# Ingest/search the PostgreSQL catalog.
java -jar target/dht-collector-java-0.1.0.jar --mode catalog --command stats
java -jar target/dht-collector-java-0.1.0.jar --mode catalog --command search \
  --query "linux image" --limit 20

# Start the dashboard (http://127.0.0.1:4173).
java -jar target/dht-collector-java-0.1.0.jar --mode dashboard \
  --http-host 127.0.0.1 --http-port 4173 --static-path ../web/public
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

The production metadata timeout is 45 seconds so the DHT peer lookup and BEP 9
handshake can complete. Keep metadata concurrency bounded instead of reducing
this timeout; short timeouts mostly turn live work into failed retries.

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
java -jar java/target/dht-collector-java-0.1.0.jar --mode approved-indexer \
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
java -jar java/target/dht-collector-java-0.1.0.jar --mode import-torrent \
  --input ./release.torrent \
  --authorization-ref publisher-release-record \
  --db ./var/dht-search.db
```

The CLI binds an ephemeral UDP port by default. Use `--port` only when a
stable port is required. Use `--no-peer-address` when output must omit peer
endpoint details.

## Java runtime

The production collector, BEP 9 metadata worker, and dashboard API have a Java
21 implementation under `java/`. It uses mldht for DHT routing and metadata
transport, virtual threads for bounded concurrent work, and HikariCP for
PostgreSQL pooling:

```bash
cd java
mvn test
mvn package
java -jar target/dht-collector-java-0.1.0.jar \
  --mode collector --port 51413 --dht-nodes 12

java -jar target/dht-collector-java-0.1.0.jar \
  --mode dashboard --http-host 127.0.0.1 --http-port 4173 \
  --static-path ../web/public
```

分布式节点、Redis 实时聚合、幂等消费和 dashboard SSE 的部署设计见
[`docs/dht-distributed-deployment.md`](docs/dht-distributed-deployment.md)。

The Java service uses the existing PostgreSQL catalog, including
`discovered_resource`, `probe_event`, `metadata_job`, `content`, and
`file_entry`. It keeps only resources seen in the last 24 hours resident,
claims metadata retries from the database, verifies the BEP 9 info dictionary,
and writes searchable content. Incoming DHT observations are coalesced by
info-hash in a bounded priority queue; fresh `announce_peer` observations are
kept ahead of `get_peers` traffic and persisted in batches of up to 256 using
one database transaction. Generic unit templates are in
`deploy/dht-passive-collector-java.service` and
`deploy/dht-search-dashboard-java.service`; adjust paths and the private
environment-file location for the target host. The environment file must stay
outside the repository.

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
- `metadata.peer.connect_timeout`
- `metadata.peer.connect_refused`
- `metadata.peer.extension_unsupported`
- `metadata.peer.metadata_timeout`
- `metadata.peer.hash_mismatch`
- `metadata.peer.penalty_skipped`
- `content.indexed`
- `collector.observation_queue_depth`
- `collector.observation_dropped`
- `collector.observation_retry`
- `collector.snapshot`
- `collector.failed`

## Current boundary

The collector only requests BEP 9 metadata and verifies the v1 infohash; it
never requests piece data. Metadata text prefers the standard UTF-8 fields and
uses charset detection for legacy names and paths. v2/hybrid identifier support
belongs in the event schema before metadata indexing is enabled.

The dashboard trend endpoint is included in `GET /api/dashboard`. It returns
five one-minute buckets for unique resource discoveries, incoming DHT queries,
actual operation failures, protocol warning occurrences, and newly indexed
contents. A warning such as
`Missing delimiter ":"` is a malformed UDP bencode packet reported by the DHT
decoder; it is aggregated separately and does not mean the collector failed to
join the network.

### Decoupled monitoring path

The collector no longer updates `minute_metric` while inserting a fact into
`probe_event`. It emits bounded, asynchronous `monitor.v1` JSON records to
stdout instead. Each record has a UTC ISO-8601 `occurred_at` ending in `Z`, for
example:

```json
{"schema":"monitor.v1","event_id":"...","service":"dht-collector","metric":"dht.query_summary","occurred_at":"2026-08-13T15:44:17.213288Z","value":12,"query":"get_peers"}
```

The local `monitor-center-agent` should follow both `tinysocket` and
`dht-passive-collector.service` journals and forward these records through its
existing spool/JetStream path. The monitor center stores raw records and
minute aggregates independently. The DHT dashboard uses the companion
`dht-monitor-ingest.service`, which reads the same journal and batches only
validated monitor records into `minute_metric`; it does not copy every monitor
line into the fact tables. The monitor center retains raw monitor records and
handles cross-source event deduplication. Fact persistence remains in the
collector transaction. Install the unit from
`deploy/dht-monitor-ingest.service` and enable it alongside the collector.

For a one-shot or manual stream, the same parser is available as:

```bash
java -jar java/target/dht-collector-java-0.1.0.jar \
  --mode monitor-ingest --input - --batch-size 128 --flush-ms 1000
```

Only `monitor.v1` records are accepted. Local timestamps without an explicit
UTC offset are rejected, preventing host timezone settings from shifting trend
buckets.

The Java `migrate-sqlite` mode uses SQLite JDBC as a one-time import/rollback
adapter. Production uses PostgreSQL with pooled connections, pre-aggregated
minute metrics, persistent counters, and concurrent job claims using
`FOR UPDATE SKIP LOCKED`.

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
java -jar java/target/dht-collector-java-0.1.0.jar --mode migrate-sqlite \
  --input ./var/dht-search.db --migration-mode full
systemctl stop dht-passive-collector.service dht-search-dashboard.service
java -jar java/target/dht-collector-java-0.1.0.jar --mode migrate-sqlite \
  --input ./var/dht-search.db --migration-mode delta
java -jar java/target/dht-collector-java-0.1.0.jar --mode migrate-sqlite \
  --input ./var/dht-search.db --migration-mode verify
```

Keep the SQLite file after cutover. To roll back, remove the `EnvironmentFile`
line from both systemd units, run `systemctl daemon-reload`, and restart them;
the existing `--db` arguments then select SQLite.

`DashboardServer.java` exposes dashboard endpoints
(`GET /api/dashboard`, paginated `GET /api/content` and `GET /api/search`,
`GET /api/health`, and the fixed service control endpoint `/api/sniffer`) and
serves the single-page console from `web/public`. Pagination uses `page` and
`page_size` query parameters and returns `total` plus `total_pages`.
