# Java DHT Collector

This module is the Java 21 implementation of the passive Mainline DHT
collector. mldht provides the Mainline DHT routing table, bootstrap, token, and
KRPC handling. Java 21 virtual threads process observations without blocking
the UDP network loop, and HikariCP provides PostgreSQL connection pooling.

Build and test:

```bash
mvn test
mvn package
```

Run with the same PostgreSQL environment used by the deployment:

```bash
java -jar target/dht-collector-java-0.1.0.jar \
  --port 51413 --dht-nodes 12 --max-concurrent 160
```

`DATABASE_URL`, `PGUSER`, and `PGPASSWORD` are read from the process
environment. Keep that environment file outside the repository. The collector
loads only active resources observed in the last 24 hours into memory and uses
the database as the source of truth for older cache misses. Repeated sightings
are flushed to PostgreSQL in bounded batches every 30 seconds. This module is
the passive DHT observation layer; BEP9 metadata fetching is still performed
by the Node.js metadata worker.
