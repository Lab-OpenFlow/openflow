package model

import (
	"time"
)

// WorkflowStatus represents the operational state of a workflow definition.
type WorkflowStatus string

const (
	WorkflowStatusDraft    WorkflowStatus = "DRAFT"
	WorkflowStatusActive   WorkflowStatus = "ACTIVE"
	WorkflowStatusArchived WorkflowStatus = "ARCHIVED"
)

// SagaStrategy controls how compensation steps are executed during rollback.
type SagaStrategy string

const (
	// SagaStrategyParallel runs all compensations concurrently (default, fastest).
	SagaStrategyParallel SagaStrategy = "parallel"
	// SagaStrategySerial runs compensations one-by-one in reverse order (use when
	// compensations have inter-dependencies, e.g. must release lock before refunding).
	SagaStrategySerial SagaStrategy = "serial"
)

// TriggerType defines how a workflow is initiated.
type TriggerType string

const (
	TriggerTypeManual        TriggerType = "manual"
	TriggerTypeWebhook       TriggerType = "webhook"
	TriggerTypeKafka         TriggerType = "kafka"
	TriggerTypeRabbitMQ      TriggerType = "rabbitmq"
	TriggerTypeCron          TriggerType = "cron"
	TriggerTypeEvent         TriggerType = "event"
	TriggerTypeChildWorkflow TriggerType = "child_workflow"
	TriggerTypeSignal        TriggerType = "signal"
	TriggerTypeTimer         TriggerType = "timer"
	// TriggerTypeRetry is used by the DLQ Worker when re-executing dead-lettered workflows.
	TriggerTypeRetry TriggerType = "retry"
)


// TriggerConfig holds trigger-specific properties.
type TriggerConfig struct {
	Type     TriggerType            `json:"type" yaml:"type"`
	Path     string                 `json:"path,omitempty" yaml:"path,omitempty"`         // For Webhook: e.g. "/webhooks/orders"
	Topic    string                 `json:"topic,omitempty" yaml:"topic,omitempty"`       // For Kafka: e.g. "orders.incoming"
	Queue    string                 `json:"queue,omitempty" yaml:"queue,omitempty"`       // For RabbitMQ: e.g. "orders_queue"
	Cron     string                 `json:"cron,omitempty" yaml:"cron,omitempty"`         // For Cron: e.g. "0 * * * *"
	Broker   string                 `json:"broker,omitempty" yaml:"broker,omitempty"`     // Broker address
	Group    string                 `json:"group,omitempty" yaml:"group,omitempty"`       // Consumer group
	Config   map[string]interface{} `json:"config,omitempty" yaml:"config,omitempty"`
}

// StageType defines the kind of node in the workflow.
type StageType string

const (
	StageTypeConnector StageType = "connector"
	StageTypeHTTP      StageType = "http"
	StageTypeKafka     StageType = "kafka"
	StageTypeRabbitMQ  StageType = "rabbitmq"
	StageTypeGRPC      StageType = "grpc"
	StageTypeWebSocket StageType = "websocket"
	StageTypeDatabase  StageType = "database"
	StageTypeTransform StageType = "transform"
	StageTypeScript    StageType = "script"
	StageTypeWASM      StageType = "wasm"
	
	// BPMN Gateway & Control Flow nodes
	StageTypeParallelFork StageType = "parallel_fork"
	StageTypeParallelJoin StageType = "parallel_join"
	StageTypeExclusiveXOR StageType = "exclusive_xor"
	StageTypeDelay        StageType = "delay"
	StageTypeSubflow      StageType = "subflow"
	StageTypeApproval     StageType = "approval"
	StageTypeWaitForSignal StageType = "wait_for_signal"
	StageTypeChildWorkflow StageType = "child_workflow"
	StageTypeWorkerTask    StageType = "worker_task"
)

// OverlapPolicy controls behavior when a scheduled cron triggers while previous execution is still running.
type OverlapPolicy string

const (
	OverlapPolicyAllowAll    OverlapPolicy = "ALLOW_ALL"
	OverlapPolicySkip        OverlapPolicy = "SKIP"
	OverlapPolicyCancelOther OverlapPolicy = "CANCEL_OTHER"
)

// RetryPolicy defines automatic retry behavior for a stage.
type RetryPolicy struct {
	MaxAttempts     int    `json:"max_attempts" yaml:"max_attempts"`
	Backoff         string `json:"backoff" yaml:"backoff"` // "constant", "linear", "exponential"
	InitialInterval string `json:"initial_interval" yaml:"initial_interval"` // e.g. "500ms", "2s"
	MaxInterval     string `json:"max_interval,omitempty" yaml:"max_interval,omitempty"`
	Multiplier      float64 `json:"multiplier,omitempty" yaml:"multiplier,omitempty"`
}

// CompensationConfig defines a rollback step executed if subsequent stages fail (Saga pattern).
type CompensationConfig struct {
	ID          string                 `json:"id" yaml:"id"`
	Name        string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Type        StageType              `json:"type" yaml:"type"`
	Config      map[string]interface{} `json:"config" yaml:"config"`
	Timeout     string                 `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

// Branch defines conditional routing for Exclusive XOR Gateways.
type Branch struct {
	Condition string `json:"condition" yaml:"condition"` // Expression, e.g. "payload.amount > 1000"
	Target    string `json:"target" yaml:"target"`       // Target stage ID
	Default   bool   `json:"default,omitempty" yaml:"default,omitempty"`
}

// Stage defines a single step or gateway in the workflow graph.
type Stage struct {
	ID           string                 `json:"id" yaml:"id"`
	Name         string                 `json:"name" yaml:"name"`
	Type         StageType              `json:"type" yaml:"type"`
	Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Config       map[string]interface{} `json:"config,omitempty" yaml:"config,omitempty"`
	Async        bool                   `json:"async,omitempty" yaml:"async,omitempty"`       // If true, fires without blocking stage completion
	Timeout      string                 `json:"timeout,omitempty" yaml:"timeout,omitempty"`   // e.g. "30s"
	Retry        *RetryPolicy           `json:"retry,omitempty" yaml:"retry,omitempty"`
	Compensation *CompensationConfig    `json:"compensation,omitempty" yaml:"compensation,omitempty"`
	Branches     []Branch               `json:"branches,omitempty" yaml:"branches,omitempty"` // For XOR gateway
	Next         []string               `json:"next,omitempty" yaml:"next,omitempty"`         // Next stage IDs
	JoinSources  []string               `json:"join_sources,omitempty" yaml:"join_sources,omitempty"` // For Parallel Join
	
	// Visual layout metadata for Studio UI
	UI *UIMetadata `json:"ui,omitempty" yaml:"ui,omitempty"`
}

// UIMetadata stores coordinates and visual appearance for the Drag & Drop editor.
type UIMetadata struct {
	PositionX float64 `json:"x" yaml:"x"`
	PositionY float64 `json:"y" yaml:"y"`
	Color     string  `json:"color,omitempty" yaml:"color,omitempty"`
	Icon      string  `json:"icon,omitempty" yaml:"icon,omitempty"`
}

// Workflow represents a complete declarative workflow definition.
type Workflow struct {
	Version      string               `json:"version" yaml:"version"`
	ID           string               `json:"id" yaml:"id"`
	Name         string               `json:"name" yaml:"name"`
	Description  string               `json:"description,omitempty" yaml:"description,omitempty"`
	Tags         []string             `json:"tags,omitempty" yaml:"tags,omitempty"`
	Status       WorkflowStatus       `json:"status" yaml:"status"`
	Trigger      *TriggerConfig       `json:"trigger,omitempty" yaml:"trigger,omitempty"`
	CronSchedule  string               `json:"cron_schedule,omitempty" yaml:"cron_schedule,omitempty"`
	OverlapPolicy OverlapPolicy        `json:"overlap_policy,omitempty" yaml:"overlap_policy,omitempty"`
	Variables    map[string]interface{} `json:"variables,omitempty" yaml:"variables,omitempty"`
	StartAt      string               `json:"start_at" yaml:"start_at"`
	Stages       []Stage              `json:"stages" yaml:"stages"`
	// SagaStrategy controls rollback execution mode. Defaults to "parallel".
	SagaStrategy SagaStrategy         `json:"saga_strategy,omitempty" yaml:"saga_strategy,omitempty"`
	CreatedAt    time.Time            `json:"created_at" yaml:"created_at"`
	UpdatedAt    time.Time            `json:"updated_at" yaml:"updated_at"`
}

// FindStage returns a stage by its ID.
func (w *Workflow) FindStage(id string) (*Stage, bool) {
	for i := range w.Stages {
		if w.Stages[i].ID == id {
			return &w.Stages[i], true
		}
	}
	return nil, false
}
