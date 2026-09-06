-- Phase 1: Add execution heartbeat and worker ownership tracking
-- Enables multi-pod safe crash recovery watchdog with SKIP LOCKED and prevents duplicate executions.

ALTER TABLE executions
  ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS worker_id    VARCHAR(255);

-- Partial index for high-throughput polling of stalled executions
CREATE INDEX IF NOT EXISTS idx_exec_stalled
  ON executions (status, heartbeat_at ASC)
  WHERE status = 'RUNNING';
