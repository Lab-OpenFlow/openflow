package storage

import (
	"context"
	"errors"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// PostgresStore implements Store interface backed by PostgreSQL with JSONB columns.
type PostgresStore struct {
	db         *sql.DB
	workflows  *PostgresWorkflowStore
	executions *PostgresExecutionStore
	audits     *PostgresAuditStore
	approvals  *PostgresApprovalStore
	timers     *PostgresTimerStore
	signals    *PostgresSignalStore
	versions   *PostgresWorkflowVersionStore
	dlq        *PostgresDLQStore
	webhooks   *PostgresWebhookStore
	tasks      *PostgresTaskQueueStore
	outbox     *PostgresOutboxStore
}

// NewPostgresStore establishes connection to PostgreSQL, migrates schemas, and returns Store.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	// Dynamic connection pool settings via environment variables (defaults for high concurrency)
	maxOpen := 50
	if envVal := os.Getenv("DB_MAX_OPEN_CONNS"); envVal != "" {
		if v, err := strconv.Atoi(envVal); err == nil && v > 0 {
			maxOpen = v
		}
	}

	maxIdle := 25
	if envVal := os.Getenv("DB_MAX_IDLE_CONNS"); envVal != "" {
		if v, err := strconv.Atoi(envVal); err == nil && v > 0 {
			maxIdle = v
		}
	}

	maxLifetime := 5 * time.Minute
	if envVal := os.Getenv("DB_CONN_MAX_LIFETIME"); envVal != "" {
		if d, err := time.ParseDuration(envVal); err == nil && d > 0 {
			maxLifetime = d
		}
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	store := &PostgresStore{
		db:         db,
		workflows:  &PostgresWorkflowStore{db: db},
		executions: &PostgresExecutionStore{db: db},
		audits:     &PostgresAuditStore{db: db},
		approvals:  &PostgresApprovalStore{db: db},
		timers:     &PostgresTimerStore{db: db},
		signals:    &PostgresSignalStore{db: db},
		versions:   &PostgresWorkflowVersionStore{db: db},
		dlq:        &PostgresDLQStore{db: db},
		webhooks:   &PostgresWebhookStore{db: db},
		tasks:      &PostgresTaskQueueStore{db: db},
		outbox:     &PostgresOutboxStore{db: db},
	}

	if err := store.AutoMigrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to migrate tables: %w", err)
	}

	slog.Info("postgresql storage initialized")
	return store, nil
}

func (s *PostgresStore) Workflows() WorkflowStore {
	return s.workflows
}

func (s *PostgresStore) Executions() ExecutionStore {
	return s.executions
}

func (s *PostgresStore) Audits() AuditStore {
	return s.audits
}

func (s *PostgresStore) Outbox() OutboxStore {
	return s.outbox
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) DB() *sql.DB {
	return s.db
}

// AutoMigrate creates all required tables and indexes if they don't exist.
func (s *PostgresStore) AutoMigrate(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS workflows (
			id VARCHAR(255) PRIMARY KEY,
			version VARCHAR(50) NOT NULL,
			name VARCHAR(255) NOT NULL,
			description TEXT,
			tags JSONB,
			status VARCHAR(50) NOT NULL,
			trigger JSONB,
			start_at VARCHAR(255) NOT NULL,
			stages JSONB NOT NULL,
			variables JSONB,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS executions (
			id VARCHAR(255) PRIMARY KEY,
			workflow_id VARCHAR(255) NOT NULL,
			workflow_name VARCHAR(255),
			trigger_type VARCHAR(50),
			status VARCHAR(50) NOT NULL,
			input JSONB,
			output JSONB,
			variables JSONB,
			error TEXT,
			trace_id VARCHAR(255),
			parent_execution_id VARCHAR(255),
			parent_stage_id VARCHAR(255),
			merkle_root VARCHAR(255),
			started_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			duration_ms BIGINT DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_exec_wf ON executions (workflow_id);`,
		`CREATE INDEX IF NOT EXISTS idx_exec_status ON executions (status);`,
		`CREATE INDEX IF NOT EXISTS idx_exec_started ON executions (started_at DESC);`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(512);`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS parent_execution_id VARCHAR(255);`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS parent_stage_id VARCHAR(255);`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS merkle_root VARCHAR(255);`,
		`CREATE INDEX IF NOT EXISTS idx_exec_parent ON executions (parent_execution_id) WHERE parent_execution_id IS NOT NULL;`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_exec_idem ON executions (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;`,
		`ALTER TABLE executions ADD COLUMN IF NOT EXISTS worker_id VARCHAR(255);`,
		`CREATE INDEX IF NOT EXISTS idx_exec_stalled ON executions (status, heartbeat_at ASC) WHERE status = 'RUNNING';`,
		`CREATE TABLE IF NOT EXISTS execution_steps (
			id VARCHAR(255) PRIMARY KEY,
			execution_id VARCHAR(255) NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
			stage_id VARCHAR(255) NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);`,
		`ALTER TABLE execution_steps ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;`,
		`CREATE INDEX IF NOT EXISTS idx_estep_exec ON execution_steps (execution_id);`,
		`CREATE INDEX IF NOT EXISTS idx_estep_stage ON execution_steps (execution_id, stage_id);`,
		`CREATE TABLE IF NOT EXISTS audit_logs (
			id VARCHAR(255) PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL,
			actor VARCHAR(255),
			action VARCHAR(255) NOT NULL,
			resource VARCHAR(255),
			resource_id VARCHAR(255),
			details JSONB,
			client_ip VARCHAR(100)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs (timestamp DESC);`,
		`CREATE TABLE IF NOT EXISTS openflow_approvals (
			id VARCHAR(255) PRIMARY KEY,
			execution_id VARCHAR(255) NOT NULL,
			workflow_id VARCHAR(255) NOT NULL,
			workflow_name VARCHAR(255),
			stage_id VARCHAR(255) NOT NULL,
			stage_name VARCHAR(255) NOT NULL,
			title VARCHAR(255),
			description TEXT,
			payload JSONB,
			required_role VARCHAR(50),
			status VARCHAR(50) NOT NULL,
			decided_by VARCHAR(255),
			decided_at TIMESTAMPTZ,
			reason TEXT,
			created_at TIMESTAMPTZ NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_openflow_approvals_status ON openflow_approvals (status);`,
		`CREATE TABLE IF NOT EXISTS openflow_timers (
			id VARCHAR(255) PRIMARY KEY,
			execution_id VARCHAR(255) NOT NULL,
			workflow_id VARCHAR(255) NOT NULL,
			stage_id VARCHAR(255) NOT NULL,
			fire_at TIMESTAMPTZ NOT NULL,
			status VARCHAR(50) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			fired_at TIMESTAMPTZ
		);`,
		`CREATE INDEX IF NOT EXISTS idx_openflow_timers_fire ON openflow_timers (status, fire_at);`,
		`CREATE TABLE IF NOT EXISTS openflow_signals (
			id VARCHAR(255) PRIMARY KEY,
			execution_id VARCHAR(255) NOT NULL,
			workflow_id VARCHAR(255) NOT NULL,
			stage_id VARCHAR(255) NOT NULL,
			signal_name VARCHAR(255) NOT NULL,
			payload JSONB,
			status VARCHAR(50) NOT NULL,
			timeout_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL,
			received_at TIMESTAMPTZ
		);`,
		`CREATE INDEX IF NOT EXISTS idx_openflow_signals_search ON openflow_signals (execution_id, signal_name, status);`,
		`CREATE TABLE IF NOT EXISTS openflow_workflow_versions (
			id VARCHAR(255) PRIMARY KEY,
			workflow_id VARCHAR(255) NOT NULL,
			version_num INT NOT NULL,
			spec JSONB NOT NULL,
			status VARCHAR(50) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_openflow_wf_versions ON openflow_workflow_versions (workflow_id, version_num DESC);`,
		`CREATE TABLE IF NOT EXISTS openflow_dlq (
			id VARCHAR(255) PRIMARY KEY,
			workflow_id VARCHAR(255) NOT NULL,
			source VARCHAR(100),
			topic_or_path VARCHAR(255),
			payload JSONB,
			headers JSONB,
			error_message TEXT,
			stack_trace TEXT,
			status VARCHAR(50) NOT NULL,
			attempts INT DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL,
			replayed_at TIMESTAMPTZ
		);`,
		`ALTER TABLE openflow_dlq ADD COLUMN IF NOT EXISTS source VARCHAR(100);`,
		`ALTER TABLE openflow_dlq ADD COLUMN IF NOT EXISTS topic_or_path VARCHAR(255);`,
		`ALTER TABLE openflow_dlq ADD COLUMN IF NOT EXISTS headers JSONB;`,
		`ALTER TABLE openflow_dlq ADD COLUMN IF NOT EXISTS stack_trace TEXT;`,
		`ALTER TABLE openflow_dlq ADD COLUMN IF NOT EXISTS attempts INT DEFAULT 1;`,
		`CREATE INDEX IF NOT EXISTS idx_openflow_dlq_status ON openflow_dlq (status);`,
		`CREATE TABLE IF NOT EXISTS openflow_webhooks (
			id VARCHAR(255) PRIMARY KEY,
			path_pattern VARCHAR(255) NOT NULL UNIQUE,
			workflow_id VARCHAR(255) NOT NULL,
			method VARCHAR(20) NOT NULL,
			secret_token VARCHAR(255),
			description TEXT,
			enabled BOOLEAN DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		);`,
		`ALTER TABLE openflow_webhooks ADD COLUMN IF NOT EXISTS description TEXT;`,
		`ALTER TABLE openflow_webhooks ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT true;`,
		`ALTER TABLE openflow_webhooks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;`,
		`CREATE TABLE IF NOT EXISTS openflow_tasks (
			id VARCHAR(255) PRIMARY KEY,
			queue_name VARCHAR(255) NOT NULL,
			workflow_id VARCHAR(255) NOT NULL,
			execution_id VARCHAR(255) NOT NULL,
			stage_id VARCHAR(255) NOT NULL,
			input JSONB,
			output JSONB,
			error_message TEXT,
			status VARCHAR(50) NOT NULL,
			worker_id VARCHAR(255),
			lock_token VARCHAR(255),
			lease_expires_at TIMESTAMPTZ,
			heartbeat_timeout_ns BIGINT NOT NULL DEFAULT 0,
			attempts INT DEFAULT 0,
			max_attempts INT DEFAULT 3,
			created_at TIMESTAMPTZ NOT NULL,
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ
		);`,
		`ALTER TABLE openflow_tasks ADD COLUMN IF NOT EXISTS heartbeat_timeout_ns BIGINT NOT NULL DEFAULT 0;`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_queue_status ON openflow_tasks (queue_name, status, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_reap ON openflow_tasks (status, lease_expires_at);`,
		`CREATE TABLE IF NOT EXISTS outbox_events (
			id VARCHAR(255) PRIMARY KEY,
			execution_id VARCHAR(255) NOT NULL,
			workflow_id VARCHAR(255) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			processed_at TIMESTAMPTZ,
			status VARCHAR(50) NOT NULL DEFAULT 'PENDING'
		);`,
		`CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (created_at ASC) WHERE status = 'PENDING';`,
	}

	for _, q := range queries {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed executing migration query (%s): %w", q, err)
		}
	}
	return nil
}

// PostgresWorkflowStore manages workflow records in PostgreSQL.
type PostgresWorkflowStore struct {
	db *sql.DB
}

func (s *PostgresWorkflowStore) Create(ctx context.Context, wf *model.Workflow) error {
	now := time.Now()
	if wf.CreatedAt.IsZero() {
		wf.CreatedAt = now
	}
	wf.UpdatedAt = now

	tagsJSON, _ := json.Marshal(wf.Tags)
	triggerJSON, _ := json.Marshal(wf.Trigger)
	stagesJSON, _ := json.Marshal(wf.Stages)
	variablesJSON, _ := json.Marshal(wf.Variables)

	query := `
		INSERT INTO workflows (id, version, name, description, tags, status, trigger, start_at, stages, variables, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET
			version = EXCLUDED.version,
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			tags = EXCLUDED.tags,
			status = EXCLUDED.status,
			trigger = EXCLUDED.trigger,
			start_at = EXCLUDED.start_at,
			stages = EXCLUDED.stages,
			variables = EXCLUDED.variables,
			updated_at = EXCLUDED.updated_at;
	`
	_, err := s.db.ExecContext(ctx, query,
		wf.ID, wf.Version, wf.Name, wf.Description, tagsJSON, string(wf.Status),
		triggerJSON, wf.StartAt, stagesJSON, variablesJSON, wf.CreatedAt, wf.UpdatedAt,
	)
	return err
}

func (s *PostgresWorkflowStore) Get(ctx context.Context, id string) (*model.Workflow, error) {
	query := `
		SELECT id, version, name, description, tags, status, trigger, start_at, stages, variables, created_at, updated_at
		FROM workflows WHERE id = $1;
	`
	row := s.db.QueryRowContext(ctx, query, id)

	var wf model.Workflow
	var statusStr string
	var tagsJSON, triggerJSON, stagesJSON, variablesJSON []byte

	err := row.Scan(
		&wf.ID, &wf.Version, &wf.Name, &wf.Description, &tagsJSON, &statusStr,
		&triggerJSON, &wf.StartAt, &stagesJSON, &variablesJSON, &wf.CreatedAt, &wf.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrWorkflowNotFound
		}
		return nil, err
	}

	wf.Status = model.WorkflowStatus(statusStr)
	_ = json.Unmarshal(tagsJSON, &wf.Tags)
	_ = json.Unmarshal(triggerJSON, &wf.Trigger)
	_ = json.Unmarshal(stagesJSON, &wf.Stages)
	_ = json.Unmarshal(variablesJSON, &wf.Variables)

	return &wf, nil
}

func (s *PostgresWorkflowStore) Update(ctx context.Context, wf *model.Workflow) error {
	return s.Create(ctx, wf)
}

func (s *PostgresWorkflowStore) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM workflows WHERE id = $1;`
	res, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrWorkflowNotFound
	}
	return nil
}

func (s *PostgresWorkflowStore) List(ctx context.Context, filter WorkflowFilter) ([]*model.Workflow, int, error) {
	var conditions []string
	var args []interface{}
	idx := 1

	if filter.TenantID != "" {
		conditions = append(conditions, fmt.Sprintf("COALESCE(tenant_id, 'default') = $%d", idx))
		args = append(args, filter.TenantID)
		idx++
	}

	if filter.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", idx))
		args = append(args, string(filter.Status))
		idx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM workflows %s;", whereClause)
	var total int
	_ = s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)

	limit := 50
	if filter.Limit > 0 {
		limit = filter.Limit
	}
	offset := filter.Offset

	query := fmt.Sprintf(`
		SELECT id, version, name, description, tags, status, trigger, start_at, stages, variables, created_at, updated_at
		FROM workflows %s
		ORDER BY updated_at DESC
		LIMIT $%d OFFSET $%d;
	`, whereClause, idx, idx+1)
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*model.Workflow
	for rows.Next() {
		var wf model.Workflow
		var statusStr string
		var tagsJSON, triggerJSON, stagesJSON, variablesJSON []byte

		err := rows.Scan(
			&wf.ID, &wf.Version, &wf.Name, &wf.Description, &tagsJSON, &statusStr,
			&triggerJSON, &wf.StartAt, &stagesJSON, &variablesJSON, &wf.CreatedAt, &wf.UpdatedAt,
		)
		if err == nil {
			wf.Status = model.WorkflowStatus(statusStr)
			_ = json.Unmarshal(tagsJSON, &wf.Tags)
			_ = json.Unmarshal(triggerJSON, &wf.Trigger)
			_ = json.Unmarshal(stagesJSON, &wf.Stages)
			_ = json.Unmarshal(variablesJSON, &wf.Variables)
			result = append(result, &wf)
		}
	}

	return result, total, nil
}

// PostgresExecutionStore manages execution records and steps in PostgreSQL.
type PostgresExecutionStore struct {
	db *sql.DB
}

func (s *PostgresExecutionStore) Create(ctx context.Context, exec *model.Execution) error {
	inputJSON, _ := json.Marshal(exec.Input)
	outputJSON, _ := json.Marshal(exec.Output)
	variablesJSON, _ := json.Marshal(exec.Variables)

	var idempKey *string
	if exec.IdempotencyKey != "" {
		idempKey = &exec.IdempotencyKey
	}

	query := `
		INSERT INTO executions (id, workflow_id, workflow_name, trigger_type, status, input, output, variables, error, trace_id, started_at, completed_at, duration_ms, idempotency_key, heartbeat_at, worker_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (id) DO NOTHING;
	`
	_, err := s.db.ExecContext(ctx, query,
		exec.ID, exec.WorkflowID, exec.WorkflowName, string(exec.TriggerType), string(exec.Status),
		inputJSON, outputJSON, variablesJSON, exec.Error, exec.TraceID,
		exec.StartedAt, exec.CompletedAt, exec.DurationMs, idempKey,
		exec.HeartbeatAt, exec.WorkerID,
	)
	return err
}

func (s *PostgresExecutionStore) Get(ctx context.Context, id string) (*model.Execution, error) {
	query := `
		SELECT e.id, e.workflow_id, e.workflow_name, e.trigger_type, e.status,
		       e.input, e.output, e.variables, e.error, e.trace_id, e.started_at,
		       e.completed_at, e.duration_ms, e.idempotency_key, e.heartbeat_at, e.worker_id,
		       COALESCE(jsonb_agg(s.payload ORDER BY s.created_at ASC) FILTER (WHERE s.id IS NOT NULL), '[]'::jsonb) AS steps_json
		FROM executions e
		LEFT JOIN execution_steps s ON e.id = s.execution_id
		WHERE e.id = $1
		GROUP BY e.id;
	`
	return s.scanExecution(ctx, query, id)
}

// GetByIdempotencyKey returns an existing execution by workflow ID and idempotency key.
func (s *PostgresExecutionStore) GetByIdempotencyKey(ctx context.Context, workflowID, key string) (*model.Execution, error) {
	if key == "" {
		return nil, ErrExecutionNotFound
	}
	query := `
		SELECT e.id, e.workflow_id, e.workflow_name, e.trigger_type, e.status,
		       e.input, e.output, e.variables, e.error, e.trace_id, e.started_at,
		       e.completed_at, e.duration_ms, e.idempotency_key, e.heartbeat_at, e.worker_id,
		       COALESCE(jsonb_agg(s.payload ORDER BY s.created_at ASC) FILTER (WHERE s.id IS NOT NULL), '[]'::jsonb) AS steps_json
		FROM executions e
		LEFT JOIN execution_steps s ON e.id = s.execution_id
		WHERE e.workflow_id = $1 AND e.idempotency_key = $2
		GROUP BY e.id
		LIMIT 1;
	`
	return s.scanExecution(ctx, query, workflowID, key)
}

// scanExecution scans one execution row and loads its aggregated steps.
func (s *PostgresExecutionStore) scanExecution(ctx context.Context, query string, args ...interface{}) (*model.Execution, error) {
	row := s.db.QueryRowContext(ctx, query, args...)

	var exec model.Execution
	var triggerStr, statusStr string
	var inputJSON, outputJSON, variablesJSON, stepsJSON []byte
	var idempKey sql.NullString
	var hb sql.NullTime
	var workerID sql.NullString

	err := row.Scan(
		&exec.ID, &exec.WorkflowID, &exec.WorkflowName, &triggerStr, &statusStr,
		&inputJSON, &outputJSON, &variablesJSON, &exec.Error, &exec.TraceID,
		&exec.StartedAt, &exec.CompletedAt, &exec.DurationMs, &idempKey,
		&hb, &workerID, &stepsJSON,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrExecutionNotFound
		}
		return nil, err
	}

	exec.TriggerType = model.TriggerType(triggerStr)
	exec.Status = model.ExecutionStatus(statusStr)
	if idempKey.Valid {
		exec.IdempotencyKey = idempKey.String
	}
	if hb.Valid {
		exec.HeartbeatAt = &hb.Time
	}
	if workerID.Valid {
		exec.WorkerID = workerID.String
	}
	_ = json.Unmarshal(inputJSON, &exec.Input)
	_ = json.Unmarshal(outputJSON, &exec.Output)
	_ = json.Unmarshal(variablesJSON, &exec.Variables)

	exec.Steps = []*model.StepExecution{}
	if len(stepsJSON) > 0 {
		_ = json.Unmarshal(stepsJSON, &exec.Steps)
	}

	return &exec, nil
}

// Update persists top-level execution metadata without modifying step rows.
func (s *PostgresExecutionStore) Update(ctx context.Context, exec *model.Execution) error {
	outputJSON, _ := json.Marshal(exec.Output)
	query := `
		UPDATE executions
		SET status = $2, output = $3, error = $4, completed_at = $5, duration_ms = $6
		WHERE id = $1;
	`
	_, err := s.db.ExecContext(ctx, query,
		exec.ID, string(exec.Status), outputJSON, exec.Error, exec.CompletedAt, exec.DurationMs,
	)
	return err
}

// AppendStep inserts a step execution into execution_steps.
func (s *PostgresExecutionStore) AppendStep(ctx context.Context, execID string, step *model.StepExecution) error {
	payload, err := json.Marshal(step)
	if err != nil {
		return fmt.Errorf("failed to marshal step payload: %w", err)
	}
	query := `
		INSERT INTO execution_steps (id, execution_id, stage_id, payload, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload;
	`
	_, err = s.db.ExecContext(ctx, query, step.ID, execID, step.StageID, payload)
	return err
}

func (s *PostgresExecutionStore) List(ctx context.Context, filter ExecutionFilter) ([]*model.Execution, int, error) {
	var conditions []string
	var args []interface{}
	idx := 1

	if filter.TenantID != "" {
		conditions = append(conditions, fmt.Sprintf("COALESCE(tenant_id, 'default') = $%d", idx))
		args = append(args, filter.TenantID)
		idx++
	}

	if filter.WorkflowID != "" {
		conditions = append(conditions, fmt.Sprintf("workflow_id = $%d", idx))
		args = append(args, filter.WorkflowID)
		idx++
	}

	if filter.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", idx))
		args = append(args, string(filter.Status))
		idx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM executions %s;", whereClause)
	var total int
	_ = s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)

	limit := 50
	if filter.Limit > 0 {
		limit = filter.Limit
	}
	offset := filter.Offset

	query := fmt.Sprintf(`
		SELECT id, workflow_id, workflow_name, trigger_type, status, input, output, variables, error, trace_id, started_at, completed_at, duration_ms
		FROM executions %s
		ORDER BY started_at DESC
		LIMIT $%d OFFSET $%d;
	`, whereClause, idx, idx+1)
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*model.Execution
	for rows.Next() {
		var exec model.Execution
		var triggerStr, statusStr string
		var inputJSON, outputJSON, variablesJSON []byte

		err := rows.Scan(
			&exec.ID, &exec.WorkflowID, &exec.WorkflowName, &triggerStr, &statusStr,
			&inputJSON, &outputJSON, &variablesJSON, &exec.Error, &exec.TraceID,
			&exec.StartedAt, &exec.CompletedAt, &exec.DurationMs,
		)
		if err == nil {
			exec.TriggerType = model.TriggerType(triggerStr)
			exec.Status = model.ExecutionStatus(statusStr)
			_ = json.Unmarshal(inputJSON, &exec.Input)
			_ = json.Unmarshal(outputJSON, &exec.Output)
			_ = json.Unmarshal(variablesJSON, &exec.Variables)
			// Note: steps are intentionally not loaded in list view (use Get for full detail)
			exec.Steps = []*model.StepExecution{}
			result = append(result, &exec)
		}
	}

	return result, total, nil
}

// PostgresAuditStore manages audit logs in PostgreSQL.
type PostgresAuditStore struct {
	db *sql.DB
}

func (s *PostgresAuditStore) Log(ctx context.Context, entry *model.AuditLogEntry) error {
	detailsJSON, _ := json.Marshal(entry.Details)
	query := `
		INSERT INTO audit_logs (id, timestamp, actor, action, resource, resource_id, details, client_ip)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
	`
	_, err := s.db.ExecContext(ctx, query,
		entry.ID, entry.Timestamp, entry.Actor, entry.Action,
		entry.Resource, entry.ResourceID, detailsJSON, entry.ClientIP,
	)
	return err
}

func (s *PostgresAuditStore) List(ctx context.Context, limit int, offset int) ([]*model.AuditLogEntry, int, error) {
	if limit <= 0 {
		limit = 50
	}
	var total int
	_ = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_logs;").Scan(&total)

	query := `
		SELECT id, timestamp, actor, action, resource, resource_id, details, client_ip
		FROM audit_logs
		ORDER BY timestamp DESC
		LIMIT $1 OFFSET $2;
	`
	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*model.AuditLogEntry
	for rows.Next() {
		var entry model.AuditLogEntry
		var detailsJSON []byte

		err := rows.Scan(
			&entry.ID, &entry.Timestamp, &entry.Actor, &entry.Action,
			&entry.Resource, &entry.ResourceID, &detailsJSON, &entry.ClientIP,
		)
		if err == nil {
			_ = json.Unmarshal(detailsJSON, &entry.Details)
			result = append(result, &entry)
		}
	}
	return result, total, nil
}

func (s *PostgresStore) Approvals() ApprovalStore               { return s.approvals }
func (s *PostgresStore) Timers() TimerStore                     { return s.timers }
func (s *PostgresStore) Signals() SignalStore                   { return s.signals }
func (s *PostgresStore) WorkflowVersions() WorkflowVersionStore { return s.versions }
func (s *PostgresStore) DLQ() DLQStore { return s.dlq }
func (s *PostgresStore) Webhooks() WebhookStore { return s.webhooks }
func (s *PostgresStore) TaskQueues() TaskQueueStore { return s.tasks }

// PostgresApprovalStore implements ApprovalStore on PostgreSQL.
type PostgresApprovalStore struct {
	db *sql.DB
}

func (p *PostgresApprovalStore) Create(ctx context.Context, req *model.ApprovalRequest) error {
	payloadJSON, _ := json.Marshal(req.Payload)
	query := `
		INSERT INTO openflow_approvals (
			id, execution_id, workflow_id, workflow_name, stage_id, stage_name,
			title, description, payload, required_role, status, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			payload = EXCLUDED.payload
	`
	_, err := p.db.ExecContext(ctx, query,
		req.ID, req.ExecutionID, req.WorkflowID, req.WorkflowName, req.StageID, req.StageName,
		req.Title, req.Description, payloadJSON, req.RequiredRole, req.Status, req.CreatedAt,
	)
	return err
}

func (p *PostgresApprovalStore) Get(ctx context.Context, id string) (*model.ApprovalRequest, error) {
	query := `
		SELECT id, execution_id, workflow_id, workflow_name, stage_id, stage_name,
		       title, description, payload, required_role, status, decided_by, decided_at, reason, created_at
		FROM openflow_approvals WHERE id = $1
	`
	var req model.ApprovalRequest
	var payloadBytes []byte
	var decBy, reason sql.NullString
	var decAt sql.NullTime

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&req.ID, &req.ExecutionID, &req.WorkflowID, &req.WorkflowName, &req.StageID, &req.StageName,
		&req.Title, &req.Description, &payloadBytes, &req.RequiredRole, &req.Status,
		&decBy, &decAt, &reason, &req.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	_ = json.Unmarshal(payloadBytes, &req.Payload)
	if decBy.Valid {
		req.DecidedBy = decBy.String
	}
	if reason.Valid {
		req.Reason = reason.String
	}
	if decAt.Valid {
		req.DecidedAt = &decAt.Time
	}

	return &req, nil
}

func (p *PostgresApprovalStore) GetByExecution(ctx context.Context, execID string) (*model.ApprovalRequest, error) {
	query := `
		SELECT id, execution_id, workflow_id, workflow_name, stage_id, stage_name,
		       title, description, payload, required_role, status, decided_by, decided_at, reason, created_at
		FROM openflow_approvals WHERE execution_id = $1 ORDER BY created_at DESC LIMIT 1
	`
	var req model.ApprovalRequest
	var payloadBytes []byte
	var decBy, reason sql.NullString
	var decAt sql.NullTime

	err := p.db.QueryRowContext(ctx, query, execID).Scan(
		&req.ID, &req.ExecutionID, &req.WorkflowID, &req.WorkflowName, &req.StageID, &req.StageName,
		&req.Title, &req.Description, &payloadBytes, &req.RequiredRole, &req.Status,
		&decBy, &decAt, &reason, &req.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	_ = json.Unmarshal(payloadBytes, &req.Payload)
	if decBy.Valid {
		req.DecidedBy = decBy.String
	}
	if reason.Valid {
		req.Reason = reason.String
	}
	if decAt.Valid {
		req.DecidedAt = &decAt.Time
	}

	return &req, nil
}

func (p *PostgresApprovalStore) Update(ctx context.Context, req *model.ApprovalRequest) error {
	query := `
		UPDATE openflow_approvals
		SET status = $1, decided_by = $2, decided_at = $3, reason = $4
		WHERE id = $5
	`
	_, err := p.db.ExecContext(ctx, query, req.Status, req.DecidedBy, req.DecidedAt, req.Reason, req.ID)
	return err
}

func (p *PostgresApprovalStore) ListPending(ctx context.Context) ([]*model.ApprovalRequest, error) {
	query := `
		SELECT id, execution_id, workflow_id, workflow_name, stage_id, stage_name,
		       title, description, payload, required_role, status, created_at
		FROM openflow_approvals
		WHERE status = 'PENDING'
		ORDER BY created_at DESC
	`
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.ApprovalRequest
	for rows.Next() {
		var req model.ApprovalRequest
		var payloadBytes []byte
		err := rows.Scan(
			&req.ID, &req.ExecutionID, &req.WorkflowID, &req.WorkflowName, &req.StageID, &req.StageName,
			&req.Title, &req.Description, &payloadBytes, &req.RequiredRole, &req.Status, &req.CreatedAt,
		)
		if err == nil {
			_ = json.Unmarshal(payloadBytes, &req.Payload)
			result = append(result, &req)
		}
	}
	return result, nil
}

func (p *PostgresApprovalStore) List(ctx context.Context, limit, offset int) ([]*model.ApprovalRequest, int, error) {
	query := `
		SELECT id, execution_id, workflow_id, workflow_name, stage_id, stage_name,
		       title, description, payload, required_role, status, decided_by, decided_at, reason, created_at
		FROM openflow_approvals
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`
	rows, err := p.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*model.ApprovalRequest
	for rows.Next() {
		var req model.ApprovalRequest
		var payloadBytes []byte
		var decBy, reason sql.NullString
		var decAt sql.NullTime
		err := rows.Scan(
			&req.ID, &req.ExecutionID, &req.WorkflowID, &req.WorkflowName, &req.StageID, &req.StageName,
			&req.Title, &req.Description, &payloadBytes, &req.RequiredRole, &req.Status,
			&decBy, &decAt, &reason, &req.CreatedAt,
		)
		if err == nil {
			_ = json.Unmarshal(payloadBytes, &req.Payload)
			if decBy.Valid {
				req.DecidedBy = decBy.String
			}
			if reason.Valid {
				req.Reason = reason.String
			}
			if decAt.Valid {
				req.DecidedAt = &decAt.Time
			}
			result = append(result, &req)
		}
	}

	var count int
	_ = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM openflow_approvals").Scan(&count)
	return result, count, nil
}


func (p *PostgresApprovalStore) GetByExecutionID(ctx context.Context, executionID string) (*model.ApprovalRequest, error) {
	query := `SELECT id, execution_id, workflow_id, workflow_name, stage_id, stage_name, title, description, payload, required_role, status, decided_by, decided_at, reason, created_at FROM openflow_approvals WHERE execution_id = $1 LIMIT 1`
	var req model.ApprovalRequest
	var payloadBytes []byte
	var decBy, reason sql.NullString
	var decAt sql.NullTime

	err := p.db.QueryRowContext(ctx, query, executionID).Scan(
		&req.ID, &req.ExecutionID, &req.WorkflowID, &req.WorkflowName, &req.StageID, &req.StageName,
		&req.Title, &req.Description, &payloadBytes, &req.RequiredRole, &req.Status,
		&decBy, &decAt, &reason, &req.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &req.Payload)
	}
	if decBy.Valid {
		req.DecidedBy = decBy.String
	}
	if reason.Valid {
		req.Reason = reason.String
	}
	if decAt.Valid {
		req.DecidedAt = &decAt.Time
	}
	return &req, nil
}

// Execution extensions

func (s *PostgresExecutionStore) ListChildren(ctx context.Context, parentExecID string) ([]*model.Execution, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, workflow_id, workflow_name, trigger_type, status, started_at, completed_at, duration_ms, parent_execution_id, parent_stage_id FROM executions WHERE parent_execution_id = $1 ORDER BY started_at ASC", parentExecID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.Execution
	for rows.Next() {
		var e model.Execution
		var completedAt sql.NullTime
		var pExecID, pStageID sql.NullString
		if err := rows.Scan(&e.ID, &e.WorkflowID, &e.WorkflowName, &e.TriggerType, &e.Status, &e.StartedAt, &completedAt, &e.DurationMs, &pExecID, &pStageID); err == nil {
			if completedAt.Valid {
				e.CompletedAt = &completedAt.Time
			}
			if pExecID.Valid {
				e.ParentExecutionID = pExecID.String
			}
			if pStageID.Valid {
				e.ParentStageID = pStageID.String
			}
			result = append(result, &e)
		}
	}
	return result, nil
}

func (s *PostgresExecutionStore) UpdateStepHeartbeat(ctx context.Context, stepID string, heartbeat time.Time) error {
	_, err := s.db.ExecContext(ctx, "UPDATE execution_steps SET heartbeat_at = $1 WHERE id = $2", heartbeat, stepID)
	return err
}

func (s *PostgresExecutionStore) UpdateHeartbeat(ctx context.Context, execID string, heartbeat time.Time) error {
	query := `UPDATE executions SET heartbeat_at = $2 WHERE id = $1;`
	_, err := s.db.ExecContext(ctx, query, execID, heartbeat)
	return err
}

func (s *PostgresExecutionStore) ClaimStalledExecutions(ctx context.Context, workerID string, stalledThreshold time.Duration, limit int) ([]*model.Execution, error) {
	if limit <= 0 {
		limit = 50
	}
	cutoff := time.Now().Add(-stalledThreshold)

	// Atomic claim via CTE with FOR UPDATE SKIP LOCKED
	query := `
		WITH claimable AS (
			SELECT id
			FROM executions
			WHERE status = 'RUNNING'
			  AND (heartbeat_at IS NULL OR heartbeat_at < $1)
			ORDER BY started_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE executions
		SET worker_id = $2, heartbeat_at = NOW()
		FROM claimable
		WHERE executions.id = claimable.id
		RETURNING executions.id, executions.workflow_id, executions.workflow_name,
		          executions.trigger_type, executions.status, executions.input,
		          executions.output, executions.variables, executions.error,
		          executions.trace_id, executions.started_at, executions.completed_at,
		          executions.duration_ms, executions.idempotency_key,
		          executions.heartbeat_at, executions.worker_id;
	`

	rows, err := s.db.QueryContext(ctx, query, cutoff, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim stalled executions: %w", err)
	}
	defer rows.Close()

	var claimed []*model.Execution
	for rows.Next() {
		var exec model.Execution
		var triggerStr, statusStr string
		var inputJSON, outputJSON, variablesJSON []byte
		var idempKey sql.NullString
		var hb sql.NullTime
		var wID sql.NullString

		err := rows.Scan(
			&exec.ID, &exec.WorkflowID, &exec.WorkflowName, &triggerStr, &statusStr,
			&inputJSON, &outputJSON, &variablesJSON, &exec.Error, &exec.TraceID,
			&exec.StartedAt, &exec.CompletedAt, &exec.DurationMs, &idempKey,
			&hb, &wID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan claimed execution: %w", err)
		}

		exec.TriggerType = model.TriggerType(triggerStr)
		exec.Status = model.ExecutionStatus(statusStr)
		if idempKey.Valid {
			exec.IdempotencyKey = idempKey.String
		}
		if hb.Valid {
			exec.HeartbeatAt = &hb.Time
		}
		if wID.Valid {
			exec.WorkerID = wID.String
		}
		_ = json.Unmarshal(inputJSON, &exec.Input)
		_ = json.Unmarshal(outputJSON, &exec.Output)
		_ = json.Unmarshal(variablesJSON, &exec.Variables)
		exec.Steps = []*model.StepExecution{}

		claimed = append(claimed, &exec)
	}

	return claimed, nil
}

// PostgresTimerStore manages durable timers in PostgreSQL.
type PostgresTimerStore struct {
	db *sql.DB
}

func (s *PostgresTimerStore) Create(ctx context.Context, timer *model.DurableTimer) error {
	query := `INSERT INTO openflow_timers (id, execution_id, workflow_id, stage_id, fire_at, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := s.db.ExecContext(ctx, query, timer.ID, timer.ExecutionID, timer.WorkflowID, timer.StageID, timer.FireAt, timer.Status, timer.CreatedAt)
	return err
}

func (s *PostgresTimerStore) Get(ctx context.Context, id string) (*model.DurableTimer, error) {
	query := `SELECT id, execution_id, workflow_id, stage_id, fire_at, status, created_at, fired_at FROM openflow_timers WHERE id = $1`
	var t model.DurableTimer
	var firedAt sql.NullTime
	err := s.db.QueryRowContext(ctx, query, id).Scan(&t.ID, &t.ExecutionID, &t.WorkflowID, &t.StageID, &t.FireAt, &t.Status, &t.CreatedAt, &firedAt)
	if err == sql.ErrNoRows {
		return nil, ErrTimerNotFound
	}
	if err != nil {
		return nil, err
	}
	if firedAt.Valid {
		t.FiredAt = &firedAt.Time
	}
	return &t, nil
}

func (s *PostgresTimerStore) ListDue(ctx context.Context, now time.Time) ([]*model.DurableTimer, error) {
	query := `SELECT id, execution_id, workflow_id, stage_id, fire_at, status, created_at, fired_at FROM openflow_timers WHERE status = 'PENDING' AND fire_at <= $1 ORDER BY fire_at ASC`
	rows, err := s.db.QueryContext(ctx, query, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.DurableTimer
	for rows.Next() {
		var t model.DurableTimer
		var firedAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.ExecutionID, &t.WorkflowID, &t.StageID, &t.FireAt, &t.Status, &t.CreatedAt, &firedAt); err == nil {
			if firedAt.Valid {
				t.FiredAt = &firedAt.Time
			}
			result = append(result, &t)
		}
	}
	return result, nil
}

func (s *PostgresTimerStore) UpdateStatus(ctx context.Context, id string, status model.TimerStatus, firedAt *time.Time) error {
	query := `UPDATE openflow_timers SET status = $1, fired_at = $2 WHERE id = $3`
	_, err := s.db.ExecContext(ctx, query, status, firedAt, id)
	return err
}

func (s *PostgresTimerStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM openflow_timers WHERE id = $1", id)
	return err
}

// PostgresSignalStore manages workflow signal subscriptions in PostgreSQL.
type PostgresSignalStore struct {
	db *sql.DB
}

func (s *PostgresSignalStore) Create(ctx context.Context, sig *model.SignalSubscription) error {
	payloadJSON, _ := json.Marshal(sig.Payload)
	query := `INSERT INTO openflow_signals (id, execution_id, workflow_id, stage_id, signal_name, payload, status, timeout_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := s.db.ExecContext(ctx, query, sig.ID, sig.ExecutionID, sig.WorkflowID, sig.StageID, sig.SignalName, payloadJSON, sig.Status, sig.TimeoutAt, sig.CreatedAt)
	return err
}

func (s *PostgresSignalStore) Get(ctx context.Context, id string) (*model.SignalSubscription, error) {
	query := `SELECT id, execution_id, workflow_id, stage_id, signal_name, payload, status, timeout_at, created_at, received_at FROM openflow_signals WHERE id = $1`
	var sig model.SignalSubscription
	var payloadBytes []byte
	var timeoutAt, receivedAt sql.NullTime
	err := s.db.QueryRowContext(ctx, query, id).Scan(&sig.ID, &sig.ExecutionID, &sig.WorkflowID, &sig.StageID, &sig.SignalName, &payloadBytes, &sig.Status, &timeoutAt, &sig.CreatedAt, &receivedAt)
	if err == sql.ErrNoRows {
		return nil, ErrSignalNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &sig.Payload)
	}
	if timeoutAt.Valid {
		sig.TimeoutAt = &timeoutAt.Time
	}
	if receivedAt.Valid {
		sig.ReceivedAt = &receivedAt.Time
	}
	return &sig, nil
}

func (s *PostgresSignalStore) ListByExecution(ctx context.Context, executionID string) ([]*model.SignalSubscription, error) {
	query := `SELECT id, execution_id, workflow_id, stage_id, signal_name, payload, status, timeout_at, created_at, received_at FROM openflow_signals WHERE execution_id = $1 ORDER BY created_at ASC`
	rows, err := s.db.QueryContext(ctx, query, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.SignalSubscription
	for rows.Next() {
		var sig model.SignalSubscription
		var payloadBytes []byte
		var timeoutAt, receivedAt sql.NullTime
		if err := rows.Scan(&sig.ID, &sig.ExecutionID, &sig.WorkflowID, &sig.StageID, &sig.SignalName, &payloadBytes, &sig.Status, &timeoutAt, &sig.CreatedAt, &receivedAt); err == nil {
			if len(payloadBytes) > 0 {
				_ = json.Unmarshal(payloadBytes, &sig.Payload)
			}
			if timeoutAt.Valid {
				sig.TimeoutAt = &timeoutAt.Time
			}
			if receivedAt.Valid {
				sig.ReceivedAt = &receivedAt.Time
			}
			result = append(result, &sig)
		}
	}
	return result, nil
}

func (s *PostgresSignalStore) FindWaiting(ctx context.Context, executionID, signalName string) (*model.SignalSubscription, error) {
	query := `SELECT id, execution_id, workflow_id, stage_id, signal_name, payload, status, timeout_at, created_at, received_at FROM openflow_signals WHERE execution_id = $1 AND signal_name = $2 AND status = 'WAITING'`
	var sig model.SignalSubscription
	var payloadBytes []byte
	var timeoutAt, receivedAt sql.NullTime
	err := s.db.QueryRowContext(ctx, query, executionID, signalName).Scan(&sig.ID, &sig.ExecutionID, &sig.WorkflowID, &sig.StageID, &sig.SignalName, &payloadBytes, &sig.Status, &timeoutAt, &sig.CreatedAt, &receivedAt)
	if err == sql.ErrNoRows {
		return nil, ErrSignalNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &sig.Payload)
	}
	if timeoutAt.Valid {
		sig.TimeoutAt = &timeoutAt.Time
	}
	if receivedAt.Valid {
		sig.ReceivedAt = &receivedAt.Time
	}
	return &sig, nil
}

func (s *PostgresSignalStore) Update(ctx context.Context, sig *model.SignalSubscription) error {
	payloadJSON, _ := json.Marshal(sig.Payload)
	query := `UPDATE openflow_signals SET payload = $1, status = $2, received_at = $3 WHERE id = $4`
	_, err := s.db.ExecContext(ctx, query, payloadJSON, sig.Status, sig.ReceivedAt, sig.ID)
	return err
}

// PostgresWorkflowVersionStore manages immutable workflow definition versions.
type PostgresWorkflowVersionStore struct {
	db *sql.DB
}

func (s *PostgresWorkflowVersionStore) CreateVersion(ctx context.Context, ver *model.WorkflowVersion) error {
	specJSON, _ := json.Marshal(ver.Spec)
	query := `INSERT INTO openflow_workflow_versions (id, workflow_id, version_num, spec, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := s.db.ExecContext(ctx, query, ver.ID, ver.WorkflowID, ver.VersionNum, specJSON, ver.Status, ver.CreatedAt)
	return err
}

func (s *PostgresWorkflowVersionStore) GetVersion(ctx context.Context, workflowID string, versionNum int) (*model.WorkflowVersion, error) {
	query := `SELECT id, workflow_id, version_num, spec, status, created_at FROM openflow_workflow_versions WHERE workflow_id = $1 AND version_num = $2`
	var ver model.WorkflowVersion
	var specBytes []byte
	err := s.db.QueryRowContext(ctx, query, workflowID, versionNum).Scan(&ver.ID, &ver.WorkflowID, &ver.VersionNum, &specBytes, &ver.Status, &ver.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrVersionNotFound
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(specBytes, &ver.Spec)
	return &ver, nil
}

func (s *PostgresWorkflowVersionStore) ListVersions(ctx context.Context, workflowID string) ([]*model.WorkflowVersion, error) {
	query := `SELECT id, workflow_id, version_num, spec, status, created_at FROM openflow_workflow_versions WHERE workflow_id = $1 ORDER BY version_num DESC`
	rows, err := s.db.QueryContext(ctx, query, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.WorkflowVersion
	for rows.Next() {
		var ver model.WorkflowVersion
		var specBytes []byte
		if err := rows.Scan(&ver.ID, &ver.WorkflowID, &ver.VersionNum, &specBytes, &ver.Status, &ver.CreatedAt); err == nil {
			_ = json.Unmarshal(specBytes, &ver.Spec)
			result = append(result, &ver)
		}
	}
	return result, nil
}

func (s *PostgresWorkflowVersionStore) GetActiveVersion(ctx context.Context, workflowID string) (*model.WorkflowVersion, error) {
	query := `SELECT id, workflow_id, version_num, spec, status, created_at FROM openflow_workflow_versions WHERE workflow_id = $1 AND status = 'ACTIVE' LIMIT 1`
	var ver model.WorkflowVersion
	var specBytes []byte
	err := s.db.QueryRowContext(ctx, query, workflowID).Scan(&ver.ID, &ver.WorkflowID, &ver.VersionNum, &specBytes, &ver.Status, &ver.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrVersionNotFound
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(specBytes, &ver.Spec)
	return &ver, nil
}

func (s *PostgresWorkflowVersionStore) SetActiveVersion(ctx context.Context, workflowID string, versionNum int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "UPDATE openflow_workflow_versions SET status = 'DEPRECATED' WHERE workflow_id = $1 AND status = 'ACTIVE'", workflowID)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, "UPDATE openflow_workflow_versions SET status = 'ACTIVE' WHERE workflow_id = $1 AND version_num = $2", workflowID, versionNum)
	if err != nil {
		return err
	}

	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrVersionNotFound
	}

	return tx.Commit()
}

// PostgresDLQStore manages dead-letter messages in PostgreSQL.
type PostgresDLQStore struct {
	db *sql.DB
}

func NewPostgresDLQStore(db *sql.DB) *PostgresDLQStore {
	return &PostgresDLQStore{db: db}
}

func (s *PostgresDLQStore) Push(ctx context.Context, msg *model.DLQMessage) error {
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("dlq_%d", time.Now().UnixNano())
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.Status == "" {
		msg.Status = model.DLQStatusPending
	}

	payloadJSON, _ := json.Marshal(msg.Payload)
	headersJSON, _ := json.Marshal(msg.Headers)

	query := `
		INSERT INTO openflow_dlq (
			id, workflow_id, source, topic_or_path, payload, headers, error_message, stack_trace, status, attempts, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			attempts = openflow_dlq.attempts + 1,
			error_message = EXCLUDED.error_message
	`
	_, err := s.db.ExecContext(ctx, query,
		msg.ID, msg.WorkflowID, msg.Source, msg.TopicOrPath, payloadJSON, headersJSON,
		msg.ErrorMessage, msg.StackTrace, string(msg.Status), msg.Attempts, msg.CreatedAt,
	)
	return err
}

func (s *PostgresDLQStore) Get(ctx context.Context, id string) (*model.DLQMessage, error) {
	query := `
		SELECT id, workflow_id, source, topic_or_path, payload, headers, error_message, stack_trace, status, attempts, created_at, replayed_at
		FROM openflow_dlq WHERE id = $1
	`
	var m model.DLQMessage
	var payloadBytes, headersBytes []byte
	var statusStr string
	var replayedAt sql.NullTime

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&m.ID, &m.WorkflowID, &m.Source, &m.TopicOrPath, &payloadBytes, &headersBytes,
		&m.ErrorMessage, &m.StackTrace, &statusStr, &m.Attempts, &m.CreatedAt, &replayedAt,
	)
	if err != nil {
		return nil, err
	}

	m.Status = model.DLQStatus(statusStr)
	_ = json.Unmarshal(payloadBytes, &m.Payload)
	if len(headersBytes) > 0 {
		_ = json.Unmarshal(headersBytes, &m.Headers)
	}
	if replayedAt.Valid {
		m.ReplayedAt = &replayedAt.Time
	}
	return &m, nil
}

func (s *PostgresDLQStore) List(ctx context.Context, filter DLQFilter) ([]*model.DLQMessage, int, error) {
	var conditions []string
	var args []interface{}
	argIdx := 1

	if filter.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(filter.Status))
		argIdx++
	}
	if filter.WorkflowID != "" {
		conditions = append(conditions, fmt.Sprintf("workflow_id = $%d", argIdx))
		args = append(args, filter.WorkflowID)
		argIdx++
	}
	if filter.Source != "" {
		conditions = append(conditions, fmt.Sprintf("source = $%d", argIdx))
		args = append(args, filter.Source)
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM openflow_dlq %s", whereClause)
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	query := fmt.Sprintf(`
		SELECT id, workflow_id, source, topic_or_path, payload, headers, error_message, stack_trace, status, attempts, created_at, replayed_at
		FROM openflow_dlq
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var messages []*model.DLQMessage
	for rows.Next() {
		var m model.DLQMessage
		var payloadBytes, headersBytes []byte
		var statusStr string
		var replayedAt sql.NullTime

		if err := rows.Scan(
			&m.ID, &m.WorkflowID, &m.Source, &m.TopicOrPath, &payloadBytes, &headersBytes,
			&m.ErrorMessage, &m.StackTrace, &statusStr, &m.Attempts, &m.CreatedAt, &replayedAt,
		); err != nil {
			return nil, 0, err
		}

		m.Status = model.DLQStatus(statusStr)
		_ = json.Unmarshal(payloadBytes, &m.Payload)
		if len(headersBytes) > 0 {
			_ = json.Unmarshal(headersBytes, &m.Headers)
		}
		if replayedAt.Valid {
			m.ReplayedAt = &replayedAt.Time
		}
		messages = append(messages, &m)
	}

	return messages, total, nil
}

func (s *PostgresDLQStore) UpdateStatus(ctx context.Context, id string, status model.DLQStatus, replayedAt *time.Time) error {
	query := `UPDATE openflow_dlq SET status = $1, replayed_at = $2 WHERE id = $3`
	_, err := s.db.ExecContext(ctx, query, string(status), replayedAt, id)
	return err
}

func (s *PostgresDLQStore) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM openflow_dlq WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

func (s *PostgresDLQStore) ListPending(ctx context.Context, limit int) ([]*model.DLQMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	query := `
		SELECT id, workflow_id, source, topic_or_path, payload, headers,
		       error_message, stack_trace, status, attempts, created_at,
		       replayed_at, last_attempt_at, is_dead, dead_reason
		FROM openflow_dlq
		WHERE status = 'PENDING' AND is_dead = FALSE
		ORDER BY created_at ASC
		LIMIT $1`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*model.DLQMessage
	for rows.Next() {
		var m model.DLQMessage
		var payloadBytes, headersBytes []byte
		var statusStr string
		var replayedAt, lastAttemptAt sql.NullTime

		if err := rows.Scan(
			&m.ID, &m.WorkflowID, &m.Source, &m.TopicOrPath, &payloadBytes, &headersBytes,
			&m.ErrorMessage, &m.StackTrace, &statusStr, &m.Attempts, &m.CreatedAt,
			&replayedAt, &lastAttemptAt, &m.IsDead, &m.DeadReason,
		); err != nil {
			return nil, err
		}
		m.Status = model.DLQStatus(statusStr)
		_ = json.Unmarshal(payloadBytes, &m.Payload)
		if len(headersBytes) > 0 {
			_ = json.Unmarshal(headersBytes, &m.Headers)
		}
		if replayedAt.Valid {
			m.ReplayedAt = &replayedAt.Time
		}
		if lastAttemptAt.Valid {
			m.LastAttemptAt = &lastAttemptAt.Time
		}
		messages = append(messages, &m)
	}
	return messages, nil
}

func (s *PostgresDLQStore) IncrementAttempts(ctx context.Context, id, errMsg string) error {
	query := `
		UPDATE openflow_dlq
		SET attempts = attempts + 1,
		    error_message = $2,
		    last_attempt_at = NOW()
		WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id, errMsg)
	return err
}

func (s *PostgresDLQStore) MarkDead(ctx context.Context, id, reason string) error {
	query := `
		UPDATE openflow_dlq
		SET is_dead = TRUE,
		    dead_reason = $2,
		    status = 'DEAD',
		    last_attempt_at = NOW()
		WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id, reason)
	return err
}

func (s *PostgresDLQStore) MarkResolved(ctx context.Context, id string) error {
	query := `
		UPDATE openflow_dlq
		SET status = 'REPLAYED',
		    replayed_at = NOW()
		WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}


// PostgresWebhookStore manages webhook bindings in PostgreSQL.
type PostgresWebhookStore struct {
	db *sql.DB
}

func NewPostgresWebhookStore(db *sql.DB) *PostgresWebhookStore {
	return &PostgresWebhookStore{db: db}
}

func (s *PostgresWebhookStore) Create(ctx context.Context, b *model.WebhookBinding) error {
	if b.ID == "" {
		b.ID = fmt.Sprintf("whk_%d", time.Now().UnixNano())
	}
	b.CreatedAt = time.Now()
	b.UpdatedAt = time.Now()

	query := `INSERT INTO openflow_webhooks (id, path_pattern, workflow_id, method, secret_token, description, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			path_pattern = EXCLUDED.path_pattern,
			workflow_id = EXCLUDED.workflow_id,
			method = EXCLUDED.method,
			secret_token = EXCLUDED.secret_token,
			description = EXCLUDED.description,
			enabled = EXCLUDED.enabled,
			updated_at = EXCLUDED.updated_at`

	_, err := s.db.ExecContext(ctx, query, b.ID, b.PathPattern, b.WorkflowID, b.Method, b.SecretToken, b.Description, b.Enabled, b.CreatedAt, b.UpdatedAt)
	return err
}

func (s *PostgresWebhookStore) Get(ctx context.Context, id string) (*model.WebhookBinding, error) {
	query := `SELECT id, path_pattern, workflow_id, method, secret_token, description, enabled, created_at, updated_at
		FROM openflow_webhooks WHERE id = $1`
	var b model.WebhookBinding
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&b.ID, &b.PathPattern, &b.WorkflowID, &b.Method, &b.SecretToken, &b.Description, &b.Enabled, &b.CreatedAt, &b.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, errors.New("webhook binding not found")
	}
	return &b, err
}

func (s *PostgresWebhookStore) FindByPath(ctx context.Context, path string, method string) (*model.WebhookBinding, error) {
	cleanPath := strings.Trim(path, "/")
	cleanPath = strings.TrimPrefix(cleanPath, "webhooks/")

	query := `SELECT id, path_pattern, workflow_id, method, secret_token, description, enabled, created_at, updated_at
		FROM openflow_webhooks WHERE enabled = true`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var b model.WebhookBinding
		if err := rows.Scan(&b.ID, &b.PathPattern, &b.WorkflowID, &b.Method, &b.SecretToken, &b.Description, &b.Enabled, &b.CreatedAt, &b.UpdatedAt); err != nil {
			continue
		}
		bClean := strings.Trim(b.PathPattern, "/")
		bClean = strings.TrimPrefix(bClean, "webhooks/")
		if bClean == cleanPath {
			if b.Method == "*" || b.Method == "" || strings.EqualFold(b.Method, method) {
				return &b, nil
			}
		}
	}
	return nil, errors.New("no matching webhook binding")
}

func (s *PostgresWebhookStore) List(ctx context.Context) ([]*model.WebhookBinding, error) {
	query := `SELECT id, path_pattern, workflow_id, method, secret_token, description, enabled, created_at, updated_at
		FROM openflow_webhooks ORDER BY created_at DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]*model.WebhookBinding, 0)
	for rows.Next() {
		var b model.WebhookBinding
		if err := rows.Scan(&b.ID, &b.PathPattern, &b.WorkflowID, &b.Method, &b.SecretToken, &b.Description, &b.Enabled, &b.CreatedAt, &b.UpdatedAt); err == nil {
			list = append(list, &b)
		}
	}
	return list, nil
}

func (s *PostgresWebhookStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM openflow_webhooks WHERE id = $1`, id)
	return err
}

// PostgresTaskQueueStore manages worker task queues in PostgreSQL.
type PostgresTaskQueueStore struct {
	db *sql.DB
}

func NewPostgresTaskQueueStore(db *sql.DB) *PostgresTaskQueueStore {
	return &PostgresTaskQueueStore{db: db}
}

func (s *PostgresTaskQueueStore) Create(ctx context.Context, t *model.TaskItem) error {
	inBytes, _ := json.Marshal(t.Input)
	outBytes, _ := json.Marshal(t.Output)

	query := `INSERT INTO openflow_tasks (id, queue_name, workflow_id, execution_id, stage_id, input, output, error_message, status, worker_id, lock_token, lease_expires_at, heartbeat_timeout_ns, attempts, max_attempts, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

	_, err := s.db.ExecContext(ctx, query,
		t.ID, t.QueueName, t.WorkflowID, t.ExecutionID, t.StageID,
		inBytes, outBytes, t.ErrorMessage, string(t.Status), t.WorkerID, t.LockToken,
		t.LeaseExpiresAt, int64(t.HeartbeatTimeout), t.Attempts, t.MaxAttempts,
		t.CreatedAt, t.StartedAt, t.CompletedAt,
	)
	return err
}

func (s *PostgresTaskQueueStore) Get(ctx context.Context, id string) (*model.TaskItem, error) {
	query := `SELECT id, queue_name, workflow_id, execution_id, stage_id, input, output, error_message, status, worker_id, lock_token, lease_expires_at, heartbeat_timeout_ns, attempts, max_attempts, created_at, started_at, completed_at
		FROM openflow_tasks WHERE id = $1`

	var t model.TaskItem
	var inBytes, outBytes []byte
	var statusStr string
	var hbNs int64

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&t.ID, &t.QueueName, &t.WorkflowID, &t.ExecutionID, &t.StageID,
		&inBytes, &outBytes, &t.ErrorMessage, &statusStr, &t.WorkerID, &t.LockToken,
		&t.LeaseExpiresAt, &hbNs, &t.Attempts, &t.MaxAttempts,
		&t.CreatedAt, &t.StartedAt, &t.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, errors.New("task not found")
	}
	if err != nil {
		return nil, err
	}

	t.Status = model.TaskStatus(statusStr)
	t.HeartbeatTimeout = time.Duration(hbNs)
	_ = json.Unmarshal(inBytes, &t.Input)
	_ = json.Unmarshal(outBytes, &t.Output)
	return &t, nil
}

func (s *PostgresTaskQueueStore) Update(ctx context.Context, t *model.TaskItem) error {
	outBytes, _ := json.Marshal(t.Output)

	query := `UPDATE openflow_tasks SET
		output = $1, error_message = $2, status = $3, worker_id = $4, lock_token = $5,
		lease_expires_at = $6, attempts = $7, started_at = $8, completed_at = $9
		WHERE id = $10`

	_, err := s.db.ExecContext(ctx, query,
		outBytes, t.ErrorMessage, string(t.Status), t.WorkerID, t.LockToken,
		t.LeaseExpiresAt, t.Attempts, t.StartedAt, t.CompletedAt, t.ID,
	)
	return err
}

func (s *PostgresTaskQueueStore) AcquirePending(ctx context.Context, queueName, workerID string, leaseDuration time.Duration) (*model.TaskItem, error) {
	now := time.Now()
	token := fmt.Sprintf("tok_%d", now.UnixNano())
	leaseExp := now.Add(leaseDuration)

	// Atomic update with SKIP LOCKED
	query := `UPDATE openflow_tasks
		SET status = 'RUNNING', worker_id = $1, lock_token = $2, lease_expires_at = $3, started_at = $4, attempts = attempts + 1
		WHERE id = (
			SELECT id FROM openflow_tasks
			WHERE queue_name = $5 AND status = 'PENDING'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, queue_name, workflow_id, execution_id, stage_id, input, output, error_message, status, worker_id, lock_token, lease_expires_at, heartbeat_timeout_ns, attempts, max_attempts, created_at, started_at, completed_at`

	var t model.TaskItem
	var inBytes, outBytes []byte
	var statusStr string
	var hbNs int64

	err := s.db.QueryRowContext(ctx, query, workerID, token, leaseExp, now, queueName).Scan(
		&t.ID, &t.QueueName, &t.WorkflowID, &t.ExecutionID, &t.StageID,
		&inBytes, &outBytes, &t.ErrorMessage, &statusStr, &t.WorkerID, &t.LockToken,
		&t.LeaseExpiresAt, &hbNs, &t.Attempts, &t.MaxAttempts,
		&t.CreatedAt, &t.StartedAt, &t.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // No pending task
	}
	if err != nil {
		return nil, err
	}

	t.Status = model.TaskStatus(statusStr)
	t.HeartbeatTimeout = time.Duration(hbNs)
	_ = json.Unmarshal(inBytes, &t.Input)
	_ = json.Unmarshal(outBytes, &t.Output)
	return &t, nil
}

func (s *PostgresTaskQueueStore) ExtendLease(ctx context.Context, taskID string, newLeaseExpiresAt time.Time) error {
	query := `UPDATE openflow_tasks SET lease_expires_at = $1 WHERE id = $2 AND status = 'RUNNING'`
	_, err := s.db.ExecContext(ctx, query, newLeaseExpiresAt, taskID)
	return err
}

func (s *PostgresTaskQueueStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	now := time.Now()
	query := `UPDATE openflow_tasks
		SET status = 'PENDING', worker_id = '', lock_token = '', lease_expires_at = NULL
		WHERE status = 'RUNNING' AND lease_expires_at < $1 AND attempts < max_attempts`

	res, err := s.db.ExecContext(ctx, query, now)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *PostgresTaskQueueStore) List(ctx context.Context, queueName string, status model.TaskStatus) ([]*model.TaskItem, error) {
	query := `SELECT id, queue_name, workflow_id, execution_id, stage_id, input, output, error_message, status, worker_id, lock_token, lease_expires_at, heartbeat_timeout_ns, attempts, max_attempts, created_at, started_at, completed_at
		FROM openflow_tasks WHERE ($1 = '' OR queue_name = $1) AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC LIMIT 100`

	rows, err := s.db.QueryContext(ctx, query, queueName, string(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]*model.TaskItem, 0)
	for rows.Next() {
		var t model.TaskItem
		var inBytes, outBytes []byte
		var statusStr string
		var hbNs int64

		if err := rows.Scan(
			&t.ID, &t.QueueName, &t.WorkflowID, &t.ExecutionID, &t.StageID,
			&inBytes, &outBytes, &t.ErrorMessage, &statusStr, &t.WorkerID, &t.LockToken,
			&t.LeaseExpiresAt, &hbNs, &t.Attempts, &t.MaxAttempts,
			&t.CreatedAt, &t.StartedAt, &t.CompletedAt,
		); err == nil {
			t.Status = model.TaskStatus(statusStr)
			t.HeartbeatTimeout = time.Duration(hbNs)
			_ = json.Unmarshal(inBytes, &t.Input)
			_ = json.Unmarshal(outBytes, &t.Output)
			list = append(list, &t)
		}
	}
	return list, nil
}

// PostgresOutboxStore manages transactional outbox events in PostgreSQL.
type PostgresOutboxStore struct {
	db *sql.DB
}

func (s *PostgresOutboxStore) Push(ctx context.Context, event *model.WorkflowEvent) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event payload: %w", err)
	}

	eventID := fmt.Sprintf("obx_%s_%d", event.ExecutionID, event.Timestamp.UnixNano())
	query := `
		INSERT INTO outbox_events (id, execution_id, workflow_id, event_type, payload, created_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
		ON CONFLICT (id) DO NOTHING;
	`
	_, err = s.db.ExecContext(ctx, query, eventID, event.ExecutionID, event.WorkflowID, string(event.Type), payloadJSON, event.Timestamp)
	return err
}

func (s *PostgresOutboxStore) FetchPending(ctx context.Context, limit int) ([]*model.WorkflowEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `
		SELECT id, execution_id, workflow_id, event_type, payload, created_at
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*model.WorkflowEvent
	for rows.Next() {
		var id, execID, wfID, eventType string
		var payloadJSON []byte
		var createdAt time.Time

		if err := rows.Scan(&id, &execID, &wfID, &eventType, &payloadJSON, &createdAt); err != nil {
			return nil, err
		}

		var payload map[string]interface{}
		_ = json.Unmarshal(payloadJSON, &payload)

		events = append(events, &model.WorkflowEvent{
			Type:        model.EventType(eventType),
			ExecutionID: execID,
			WorkflowID:  wfID,
			Timestamp:   createdAt,
			Payload:     payload,
		})
	}

	return events, nil
}

func (s *PostgresOutboxStore) MarkProcessed(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf("UPDATE outbox_events SET status = 'PROCESSED', processed_at = NOW() WHERE id IN (%s);", strings.Join(placeholders, ", "))
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

