CREATE INDEX CONCURRENTLY IF NOT EXISTS content_name_search_idx
  ON content USING gin (to_tsvector('simple', coalesce(name, '')))
  WHERE policy_state = 'approved';

CREATE INDEX CONCURRENTLY IF NOT EXISTS file_entry_path_search_idx
  ON file_entry USING gin (to_tsvector('simple', coalesce(path, '')));

CREATE INDEX CONCURRENTLY IF NOT EXISTS metadata_job_recent_due_idx
  ON metadata_job (priority DESC, updated_at DESC, next_attempt_at ASC);
