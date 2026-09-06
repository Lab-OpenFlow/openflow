-- Rollback Sprint 1 DLQ retry fields migration
DROP INDEX IF EXISTS idx_dlq_worker_pending;

ALTER TABLE openflow_dlq
  DROP COLUMN IF EXISTS last_attempt_at,
  DROP COLUMN IF EXISTS is_dead,
  DROP COLUMN IF EXISTS dead_reason;
