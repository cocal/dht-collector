#!/usr/bin/env bash
set -euo pipefail

# Keep the retry queue's visibility map and planner statistics current without
# taking an application-level lock or stopping the collector.
export PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"
exec /usr/bin/psql -v ON_ERROR_STOP=1 -c 'VACUUM (ANALYZE) metadata_job'
