-- Sprint 1 (GAP #9): Add DLQ auto-retry tracking fields
-- Extends the openflow_dlq table to support DLQWorker's exponential backoff and permanent death marking.

ALTER TABLE openflow_dlq
  ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS is_dead         BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS dead_reason     TEXT;

-- Partial index for efficient DLQWorker polling (only scans live, retryable messages)
CREATE INDEX IF NOT EXISTS idx_dlq_worker_pending
  ON openflow_dlq (created_at ASC)
  WHERE status = 'PENDING' AND is_dead = FALSE;
