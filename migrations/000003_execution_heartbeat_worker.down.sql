DROP INDEX IF EXISTS idx_exec_stalled;

ALTER TABLE executions
  DROP COLUMN IF EXISTS heartbeat_at,
  DROP COLUMN IF EXISTS worker_id;
