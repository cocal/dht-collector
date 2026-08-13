# Monitor Center Integration

The DHT collector publishes only structured `monitor.v1` records for the
monitoring path. The service manager writes each line to the
`dht-passive-collector.service` journal; the existing monitor-center agent
forwards journal records through its local spool and JetStream publisher.

The agent configuration needs both journal units, with the DHT unit restricted
to structured records so startup and database messages are not forwarded as
metrics:

```ini
MONITOR_CENTER_JOURNAL_UNIT=tinysocket,dht-passive-collector.service
MONITOR_CENTER_MONITOR_ONLY_UNITS=dht-passive-collector.service
```

The monitor-center consumer should parse the JSON in the journal `MESSAGE`
field, require `schema=monitor.v1`, validate `occurred_at` with an explicit
UTC offset (the collector emits `Z`), and use `event_id` for idempotency. Its
raw event table and minute aggregate table are separate from DHT fact tables.

The local DHT dashboard has a projection worker,
`dht-monitor-ingest.service`, that reads the same journal and writes only
minute aggregates to the DHT catalog. This keeps the dashboard responsive if
the remote monitor center or its JetStream connection is unavailable; the
remote center remains the durable raw-log destination.

No monitor-center credentials, endpoints, host addresses, or private keys are
stored in this repository. They remain in the host environment file.
