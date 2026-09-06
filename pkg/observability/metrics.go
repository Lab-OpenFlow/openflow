package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus collectors for orchestrator telemetry.
type Metrics struct {
	// ── LATENCY ─────────────────────────────────────────────────────────────────
	// P50/P95/P99 of end-to-end workflow execution time, segmented by workflow and trigger.
	ExecutionDuration *prometheus.HistogramVec
	// Per-stage execution time, segmented by connector type and outcome.
	StageDuration *prometheus.HistogramVec
	// How long external workers wait for a task to be available (poll latency).
	TaskQueueLatency *prometheus.HistogramVec

	// ── TRAFFIC ─────────────────────────────────────────────────────────────────
	// Rate of new executions triggered, by workflow and trigger source.
	ExecutionsStarted *prometheus.CounterVec
	// Rate of executions completing (any final state), by workflow and final status.
	ExecutionsFinished *prometheus.CounterVec
	// Rate of individual stages executed, by type and outcome.
	StagesExecuted *prometheus.CounterVec

	// ── ERRORS ──────────────────────────────────────────────────────────────────
	// Execution failures segmented by workflow and error category.
	ExecutionsFailed *prometheus.CounterVec
	// Saga rollback events, by workflow and compensation strategy.
	SagaRollbacks *prometheus.CounterVec
	// Circuit breaker state transitions.
	CircuitBreakerTrips *prometheus.CounterVec
	// Messages pushed to DLQ, by workflow and failure reason.
	DLQMessagesTotal *prometheus.CounterVec
	// DLQ retry outcomes, by workflow.
	DLQMessagesRetried *prometheus.CounterVec
	// Idempotency cache hits.
	IdempotencyHits prometheus.Counter
	// Slow event listener drops (exceeded 50ms fan-out deadline).
	SlowListenerDrops prometheus.Counter

	// ── SATURATION ──────────────────────────────────────────────────────────────
	// Current concurrently running workflow goroutines (existing).
	ActiveExecutions prometheus.Gauge
	// Fraction of the engine semaphore slots in use (0.0–1.0).
	// Approaching 1.0 means the engine is at capacity and will queue new executions.
	SemaphoreUtilization prometheus.Gauge
	// Number of pending tasks waiting in each named worker queue.
	TaskQueueDepth *prometheus.GaugeVec
	// Fraction of the internal eventChan buffer that is currently filled (0.0–1.0).
	// Approaching 1.0 means WebSocket/event consumers are lagging behind production.
	EventChannelFillRatio prometheus.Gauge
	// Total executions auto-recovered by the crash-recovery watchdog.
	WatchdogRecoveries prometheus.Counter

	// ── CLUSTER ─────────────────────────────────────────────────────────────────
	// Leader election lifecycle events: acquired, lost, failed.
	LeaderElectionEvents *prometheus.CounterVec
	// Number of live nodes currently in the consistent hash ring.
	HashRingNodes prometheus.Gauge
	// Executions forwarded to the shard-owner node via gRPC, by target and result.
	ClusterForwardedExecs *prometheus.CounterVec
}

// GlobalMetrics is the process-wide singleton, initialised once at startup.
var GlobalMetrics = newMetrics()

func newMetrics() *Metrics {
	return &Metrics{
		// ── LATENCY ─────────────────────────────────────────────────────────────
		ExecutionDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "openflow_execution_duration_seconds",
				Help:    "End-to-end workflow execution duration in seconds.",
				Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
			},
			[]string{"workflow_id", "trigger_type", "status"},
		),
		StageDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "openflow_stage_duration_seconds",
				Help:    "Individual stage execution duration in seconds.",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"stage_type", "status"},
		),
		TaskQueueLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "openflow_task_queue_poll_wait_seconds",
				Help:    "Time an external worker waits for a task to become available.",
				Buckets: []float64{.1, .5, 1, 2, 5, 10, 20, 30},
			},
			[]string{"queue_name"},
		),

		// ── TRAFFIC ─────────────────────────────────────────────────────────────
		ExecutionsStarted: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_executions_started_total",
				Help: "Total workflow executions triggered.",
			},
			[]string{"workflow_id", "trigger_type"},
		),
		ExecutionsFinished: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_executions_finished_total",
				Help: "Total workflow executions that reached a terminal state.",
			},
			[]string{"workflow_id", "status"},
		),
		StagesExecuted: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_stages_executed_total",
				Help: "Total individual stage executions.",
			},
			[]string{"stage_type", "status"},
		),

		// ── ERRORS ──────────────────────────────────────────────────────────────
		ExecutionsFailed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_executions_failed_total",
				Help: "Total failed workflow executions by error category.",
			},
			[]string{"workflow_id", "error_category"},
		),
		SagaRollbacks: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_saga_rollbacks_total",
				Help: "Total saga compensation rollbacks triggered.",
			},
			[]string{"workflow_id", "strategy"},
		),
		CircuitBreakerTrips: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_circuit_breaker_trips_total",
				Help: "Total circuit breaker state transitions to OPEN.",
			},
			[]string{"connector_key"},
		),
		DLQMessagesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_dlq_messages_total",
				Help: "Total messages pushed to the Dead Letter Queue.",
			},
			[]string{"workflow_id", "reason"},
		),
		DLQMessagesRetried: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_dlq_retried_total",
				Help: "DLQ auto-retry outcomes by result (success/failed/dead).",
			},
			[]string{"workflow_id", "result"},
		),
		IdempotencyHits: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "openflow_idempotency_hits_total",
				Help: "Bloom Filter hits that short-circuited idempotency DB queries.",
			},
		),
		SlowListenerDrops: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "openflow_slow_listener_drops_total",
				Help: "Event fan-out drops due to listener exceeding 50ms deadline.",
			},
		),

		// ── SATURATION ──────────────────────────────────────────────────────────
		ActiveExecutions: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "openflow_active_executions",
				Help: "Current number of concurrently running workflow executions.",
			},
		),
		SemaphoreUtilization: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "openflow_semaphore_utilization",
				Help: "Fraction of engine concurrency slots in use (0.0–1.0). Alert at >0.9.",
			},
		),
		TaskQueueDepth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "openflow_task_queue_depth",
				Help: "Number of pending tasks awaiting an external worker.",
			},
			[]string{"queue_name"},
		),
		EventChannelFillRatio: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "openflow_event_channel_fill_ratio",
				Help: "Fraction of the event fan-out channel buffer currently filled (0.0–1.0).",
			},
		),
		WatchdogRecoveries: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "openflow_watchdog_recoveries_total",
				Help: "Total workflow executions auto-recovered by the crash-recovery watchdog.",
			},
		),

		// ── CLUSTER ─────────────────────────────────────────────────────────────
		LeaderElectionEvents: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_leader_election_events_total",
				Help: "Leader election lifecycle events (acquired / lost / failed).",
			},
			[]string{"event"},
		),
		HashRingNodes: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "openflow_hash_ring_nodes",
				Help: "Number of live nodes in the consistent hash ring.",
			},
		),
		ClusterForwardedExecs: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "openflow_cluster_forwarded_executions_total",
				Help: "Executions forwarded to the shard-owner node via gRPC.",
			},
			[]string{"target_node", "result"},
		),
	}
}
