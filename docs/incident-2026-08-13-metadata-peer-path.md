# Incident: Metadata Ingestion Gap During Java Migration

Date: 2026-08-13

## Symptom

The Java collector continued to receive DHT queries and discover info-hashes,
but indexed `content` stopped growing. Metadata attempts mostly ended with
`no metadata result`, while the previous Node collector was able to index live
resources.

## Root cause

The Node implementation used the endpoint carried by each `announce_peer`
request and immediately attempted a BitTorrent metadata connection. The first
Java implementation retained only the info-hash and started a new DHT
`get_peers` lookup. That discarded the live peer hint and made metadata
retrieval dependent on a second lookup, which often returned no usable peer.

The endpoint was also not written to the structured `probe_event` peer fields,
so the missing handoff was harder to diagnose after a restart.

## Required invariants for future runtime replacements

1. Preserve the direct `announce_peer` endpoint path and try it immediately.
2. Keep DHT lookup as a fallback, not as a replacement for the direct path.
3. Persist the selected peer and DHT source in `probe_event.peer_host`,
   `peer_port`, `source_host`, and `source_port`, as well as in `raw_event`.
4. Keep metadata hash verification and bounded concurrency unchanged.
5. Validate a replacement with an end-to-end check: announce peer received,
   peer event persisted, metadata completion emitted, and `content` inserted.

## Fix

The Java collector now records each announce peer, starts metadata work from
the most recent announce endpoints, and falls back to `TorrentFetcher` DHT
lookup. The fix was verified in production with new `metadata.fetch_completed`
events and new `content` rows after deployment.
