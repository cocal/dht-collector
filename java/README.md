# Java DHT Runtime

This module is the Java 21 runtime for the Mainline DHT collector, BEP 9
metadata worker, and dashboard API. mldht provides the routing table, KRPC, and
metadata transport. Java 21 virtual threads process observations and metadata
jobs without blocking the UDP network loop, and HikariCP provides PostgreSQL
connection pooling.

Build and test:

```bash
mvn test
mvn package
```

Run with the same PostgreSQL environment used by the deployment:

```bash
java -jar target/dht-collector-java-0.1.0.jar \
  --mode collector --port 51413 --dht-nodes 12 --max-concurrent 160 \
  --metadata-concurrent 8 --metadata-timeout-seconds 12
```

Run the dashboard and static web UI from the same jar:

```bash
java -jar target/dht-collector-java-0.1.0.jar \
  --mode dashboard --http-host 127.0.0.1 --http-port 4173 \
  --static-path ../web/public
```

The same jar also replaces the former Node CLI tools:

```bash
java -jar target/dht-collector-java-0.1.0.jar --mode catalog --command stats
java -jar target/dht-collector-java-0.1.0.jar --mode catalog --command search --query linux
java -jar target/dht-collector-java-0.1.0.jar --mode metadata-worker --input events.jsonl
java -jar target/dht-collector-java-0.1.0.jar --mode approved-indexer --input approved.jsonl
java -jar target/dht-collector-java-0.1.0.jar --mode import-torrent \
  --input release.torrent --authorization-ref publisher-record
java -jar target/dht-collector-java-0.1.0.jar --mode migrate-sqlite \
  --input ./var/dht-search.db --migration-mode verify
java -jar target/dht-collector-java-0.1.0.jar --mode monitor-ingest \
  --journal-unit dht-passive-collector.service --batch-size 128 --flush-ms 1000
```

`DATABASE_URL`, `PGUSER`, and `PGPASSWORD` are read from the process
environment. Keep that environment file outside the repository. The collector
keeps a bounded cache of active resources observed in the last 2 hours and uses
the database as the source of truth for older cache misses. Repeated sightings
are flushed to PostgreSQL in bounded batches every 30 seconds. The metadata
worker claims persisted jobs, verifies the returned info dictionary against
the requested infohash, and upserts content and file rows. The dashboard reads
the same PostgreSQL catalog through its own bounded connection pool.

Trend metrics are decoupled from fact writes. The collector emits asynchronous
`monitor.v1` JSON lines with UTC `occurred_at` timestamps; the journal agent and
the independent `monitor-ingest` mode parse and aggregate them into minute
buckets without copying each monitor line into the fact tables. The collector
does not write `minute_metric` in its event transaction.
