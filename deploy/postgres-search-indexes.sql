CREATE INDEX CONCURRENTLY IF NOT EXISTS content_name_search_idx
  ON content USING gin (to_tsvector('simple', coalesce(name, '')))
  WHERE policy_state = 'approved';

-- Search content names and file paths without joining millions of file_entry rows.
-- The split expressions avoid PostgreSQL's 1 MB tsvector limit for large torrents.
CREATE INDEX CONCURRENTLY IF NOT EXISTS content_search_head_idx
  ON content USING gin (
    to_tsvector('simple', left(coalesce(name, '') || ' ' || coalesce(files_text, ''), 800000))
  ) WHERE policy_state = 'approved';

CREATE INDEX CONCURRENTLY IF NOT EXISTS content_search_tail_idx
  ON content USING gin (
    to_tsvector('simple', substring(coalesce(files_text, '') from 700001 for 800000))
  ) WHERE policy_state = 'approved' AND length(files_text) > 700000;

CREATE INDEX CONCURRENTLY IF NOT EXISTS file_entry_path_search_idx
  ON file_entry USING gin (to_tsvector('simple', coalesce(path, '')));

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_job_recent_due_idx
  ON metadata_job (priority DESC, updated_at DESC, next_attempt_at ASC);

-- The worker only claims pending jobs. Keeping that predicate in the index avoids
-- scanning millions of completed/processing rows on every small claim batch.
CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_job_pending_due_idx
  ON metadata_job (priority DESC, updated_at DESC, next_attempt_at ASC, info_hash)
  WHERE status = 'pending';

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_job_pending_next_idx
  ON metadata_job (next_attempt_at ASC, priority DESC, updated_at DESC, info_hash)
  WHERE status = 'pending';

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_job_processing_lock_idx
  ON metadata_job (locked_until, info_hash)
  WHERE status = 'processing';

-- Recover short-lived announce peer hints without scanning unrelated probe events.
CREATE INDEX CONCURRENTLY IF NOT EXISTS probe_event_recent_peer_idx
  ON probe_event (info_hash, occurred_at DESC)
  INCLUDE (peer_host, peer_port, source_host, source_port)
  WHERE event_type = 'dht.peer_discovered' AND peer_host IS NOT NULL;
