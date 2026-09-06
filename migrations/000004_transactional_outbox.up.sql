-- Phase 3 (Feature #1): Transactional Outbox Pattern for guaranteed at-least-once event delivery
CREATE TABLE IF NOT EXISTS outbox_events (
    id           VARCHAR(255) PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    workflow_id  VARCHAR(255) NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    status       VARCHAR(50) NOT NULL DEFAULT 'PENDING'
);

-- Partial index for high-throughput concurrent drain by OutboxRelay using SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_outbox_pending
  ON outbox_events (created_at ASC)
  WHERE status = 'PENDING';
