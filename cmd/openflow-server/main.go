package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/api"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connDB "github.com/Lab-OpenFlow/openflow/pkg/connectors/database"
	connDMN "github.com/Lab-OpenFlow/openflow/pkg/connectors/dmn"
	connGRPC "github.com/Lab-OpenFlow/openflow/pkg/connectors/grpc"
	connHTTP "github.com/Lab-OpenFlow/openflow/pkg/connectors/http"
	connKafka "github.com/Lab-OpenFlow/openflow/pkg/connectors/kafka"
	connRabbit "github.com/Lab-OpenFlow/openflow/pkg/connectors/rabbitmq"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	connWasm "github.com/Lab-OpenFlow/openflow/pkg/connectors/wasm"
	connWS "github.com/Lab-OpenFlow/openflow/pkg/connectors/websocket"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
	"github.com/Lab-OpenFlow/openflow/pkg/security"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
	"github.com/Lab-OpenFlow/openflow/pkg/ui"

	"gopkg.in/yaml.v3"
)

func main() {
	fmt.Print(`
  ___                    _____ _                 
 / _ \ _ __   ___ _ __  |  ___| | _____      __  
| | | | '_ \ / _ \ '_ \ | |_  | |/ _ \ \ /\ / /  
| |_| | |_) |  __/ | | ||  _| | | (_) \ V  V /   
 \___/| .__/ \___|_| |_||_|   |_|\___/ \_/\_/    
      |_|                                        
 OpenFlow Orchestrator v2.0.0
`)

	port := os.Getenv("PORT")
	if port == "" {
		port = ":8080"
	} else if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	// 0. Initialize Modern Structured Logger & OpenTelemetry Distributed Tracing
	observability.InitLogger()
	tp, err := observability.InitTracer(context.Background(), "openflow-orchestrator")
	if err == nil && tp != nil {
		defer func() {
			_ = tp.Shutdown(context.Background())
		}()
	}

	// 1. Initialize Storage (PostgreSQL with Memory Fallback)
	var store storage.Store
	pgDSN := os.Getenv("POSTGRES_DSN")
	if pgDSN != "" {
		pgStore, err := storage.NewPostgresStore(pgDSN)
		if err != nil {
			slog.Warn("Failed to connect to PostgreSQL. Falling back to in-memory store.",
				slog.String("dsn", pgDSN),
				slog.String("error", err.Error()),
			)
			store = storage.NewMemoryStore()
		} else {
			store = pgStore
			defer pgStore.Close()
			if security.GlobalVault != nil {
				_ = security.GlobalVault.AttachDB(pgStore.DB())
			}
		}
	} else {
		slog.Info("Using In-Memory Store (Set POSTGRES_DSN for PostgreSQL database persistence)")
		store = storage.NewMemoryStore()
	}

	// 2. Initialize and Register Connectors
	kafkaBrokersEnv := os.Getenv("KAFKA_BROKERS")
	var kafkaBrokers []string
	if kafkaBrokersEnv != "" {
		for _, b := range strings.Split(kafkaBrokersEnv, ",") {
			if s := strings.TrimSpace(b); s != "" {
				kafkaBrokers = append(kafkaBrokers, s)
			}
		}
	}
	if len(kafkaBrokers) == 0 {
		kafkaBrokers = []string{"localhost:9092"}
	}

	rabbitURLEnv := os.Getenv("RABBITMQ_URL")
	if rabbitURLEnv == "" {
		rabbitURLEnv = "amqp://guest:guest@localhost:5672/"
	}

	registry := connectors.NewRegistry()
	registry.Register(connHTTP.NewHTTPConnector())
	registry.Register(connKafka.NewKafkaConnector(kafkaBrokers))
	registry.Register(connRabbit.NewRabbitMQConnector(rabbitURLEnv))
	registry.Register(connTransform.NewTransformConnector())
	registry.Register(connDMN.NewDMNConnector())
	registry.Register(connDB.NewDatabaseConnector())
	registry.Register(connWS.NewWebSocketConnector())
	registry.Register(connGRPC.NewGRPCConnector())
	registry.Register(connWasm.NewWASMConnector())

	// 3. Initialize Core Workflow Engine
	eng := engine.NewEngine(store, registry)

	// 3.1 Initialize Crash Recovery Watchdog (Durable Execution)
	watchdog := engine.NewRecoveryWatchdog(eng, store)
	go watchdog.Start(context.Background(), 30*time.Second)

	// 3.2 Initialize Durable Timer Scheduler (persisted sleep & delay)
	timerSched := engine.NewDurableTimerScheduler(eng, store)
	timerSched.Start()
	defer timerSched.Stop()

	// 4. Initialize Trigger Manager for external inbound event streams
	triggerMgr := engine.NewTriggerManager(eng)
	defer triggerMgr.Stop()

	// 5. Initialize Security & Auth Manager
	authMgr := security.NewAuthManager(os.Getenv("OPENFLOW_JWT_SECRET"))

	// 6. Seed Built-in Example Workflows
	seedWorkflows(store)

	// Register triggers for all active workflows in store (Kafka consumers, RabbitMQ, Webhooks)
	activeWfs, _, _ := store.Workflows().List(context.Background(), storage.WorkflowFilter{Status: model.WorkflowStatusActive})
	for _, w := range activeWfs {
		_ = triggerMgr.RegisterWorkflowTriggers(w)
	}

	// 7. Initialize API & WebSocket Server
	staticFS, _ := ui.GetFS()
	cfg := api.ServerConfig{
		Port:        port,
		StaticFS:    staticFS,
		EnableAuth:  false,
		AuthManager: authMgr,
	}
	server := api.NewServer(cfg, eng, store, registry)

	slog.Info("server starting",
		slog.String("port", port),
		slog.String("metrics", fmt.Sprintf("http://localhost%s/metrics", port)),
		slog.String("health", fmt.Sprintf("http://localhost%s/health", port)),
	)

	// Graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != nil {
			slog.Error("server failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("initiating graceful shutdown")
	triggerMgr.Stop()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()
	_ = eng.Drain(drainCtx, 10*time.Second)
	_ = store.Close()
	slog.Info("server stopped")
}

func seedWorkflows(store storage.Store) {
	ctx := context.Background()

	// Only seed if workflow does NOT already exist in database!
	if _, err := store.Workflows().Get(ctx, "kafka-pipeline"); err == storage.ErrWorkflowNotFound {
		// Sample 1: Kafka -> Transform -> HTTP Pipeline
		wf1 := &model.Workflow{
			Version:     "v1",
			ID:          "kafka-pipeline",
			Name:        "Kafka Data Ingestion & Transformation Pipeline",
			Description: "Ingests raw IoT telemetry from Kafka topic, filters anomalies with Go expressions, and notifies downstream API",
			Tags:        []string{"iot", "kafka", "telemetry", "pipeline"},
			Status:      model.WorkflowStatusActive,
			Trigger: &model.TriggerConfig{
				Type:  model.TriggerTypeWebhook,
				Path:  "/webhooks/iot-sensor",
				Topic: "telemetry.sensors",
			},
			StartAt: "validate_telemetry",
			Stages: []model.Stage{
				{
					ID:   "validate_telemetry",
					Name: "Validate Telemetry Payload",
					Type: model.StageTypeTransform,
					Config: map[string]interface{}{
						"expression": "payload.temperature > -50 && payload.temperature < 150",
						"mapping": map[string]interface{}{
							"device_id":   "payload.device_id",
							"temp_c":      "payload.temperature",
							"temp_f":      "payload.temperature * 1.8 + 32",
							"is_critical": "payload.temperature > 85",
						},
					},
					Next: []string{"check_temperature"},
					UI:   &model.UIMetadata{PositionX: 100, PositionY: 150, Icon: "Cpu"},
				},
				{
					ID:   "check_temperature",
					Name: "Check Critical Threshold (Exclusive XOR)",
					Type: model.StageTypeExclusiveXOR,
					Branches: []model.Branch{
						{
							Condition: "payload.is_critical == true",
							Target:    "emit_kafka_alert",
						},
						{
							Default: true,
							Target:  "save_to_database",
						},
					},
					UI: &model.UIMetadata{PositionX: 350, PositionY: 150, Icon: "GitBranch"},
				},
				{
					ID:   "emit_kafka_alert",
					Name: "Publish Critical Alert to Kafka",
					Type: model.StageTypeKafka,
					Config: map[string]interface{}{
						"topic": "alerts.critical",
						"key":   "{{.payload.device_id}}",
						"value": map[string]interface{}{
							"alert":       "CRITICAL_TEMPERATURE_EXCEEDED",
							"device_id":   "{{.payload.device_id}}",
							"temp_c":      "{{.payload.temp_c}}",
							"temp_f":      "{{.payload.temp_f}}",
							"recorded_at": "{{.payload.timestamp}}",
						},
					},
					Next: []string{"save_to_database"},
					UI:   &model.UIMetadata{PositionX: 600, PositionY: 80, Icon: "Layers"},
				},
				{
					ID:   "save_to_database",
					Name: "Archive Telemetry Record in DB",
					Type: model.StageTypeDatabase,
					Config: map[string]interface{}{
						"driver": "postgres",
						"dsn":    "mock://db",
						"query":  "INSERT INTO telemetry_logs (device_id, temp_c) VALUES ($1, $2)",
					},
					UI: &model.UIMetadata{PositionX: 600, PositionY: 220, Icon: "Database"},
				},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_ = store.Workflows().Create(ctx, wf1)
	}

	if _, err := store.Workflows().Get(ctx, "financial-transfer-saga"); err == storage.ErrWorkflowNotFound {
		// Sample 2: Financial Transaction Saga with Compensations / Rollback
	wf2 := &model.Workflow{
		Version:     "v1",
		ID:          "financial-transfer-saga",
		Name:        "Financial Bank Transfer with Saga Rollback",
		Description: "Multi-stage financial transfer with backward compensation: Deduct Account A, Credit Account B, and Rollback on failure",
		Tags:        []string{"fintech", "saga", "banking", "compensation"},
		Status:      model.WorkflowStatusActive,
		Trigger: &model.TriggerConfig{
			Type: model.TriggerTypeWebhook,
			Path: "/webhooks/transfer",
		},
		StartAt: "deduct_sender_account",
		Stages: []model.Stage{
			{
				ID:   "deduct_sender_account",
				Name: "Deduct Sender Account Balance",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"expression": "payload.amount > 0",
					"mapping": map[string]interface{}{
						"transaction_id": "payload.transaction_id",
						"sender_id":      "payload.sender_id",
						"receiver_id":    "payload.receiver_id",
						"amount":         "payload.amount",
						"sender_debited": true,
					},
				},
				Compensation: &model.CompensationConfig{
					ID:   "refund_sender_account",
					Name: "Refund Sender Account Balance (Rollback)",
					Type: model.StageTypeTransform,
					Config: map[string]interface{}{
						"mapping": map[string]interface{}{
							"action":   "REFUND_SENDER",
							"account":  "{{.payload.sender_id}}",
							"amount":   "{{.payload.amount}}",
							"refunded": true,
						},
					},
				},
				Next: []string{"credit_receiver_account"},
				UI:   &model.UIMetadata{PositionX: 100, PositionY: 150, Icon: "DollarSign"},
			},
			{
				ID:   "credit_receiver_account",
				Name: "Credit Receiver Account Balance",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"receiver_credited": true,
						"credited_amount":   "payload.amount",
						"status":            "'SUCCESS'",
					},
				},
				Compensation: &model.CompensationConfig{
					ID:   "revert_receiver_credit",
					Name: "Revert Receiver Credit (Rollback)",
					Type: model.StageTypeTransform,
					Config: map[string]interface{}{
						"mapping": map[string]interface{}{
							"action":   "REVERT_CREDIT",
							"account":  "{{.payload.receiver_id}}",
							"reverted": true,
						},
					},
				},
				Next: []string{"publish_transaction_event"},
				UI:   &model.UIMetadata{PositionX: 350, PositionY: 150, Icon: "CheckCircle"},
			},
			{
				ID:   "publish_transaction_event",
				Name: "Publish TransactionCompleted Event",
				Type: model.StageTypeKafka,
				Config: map[string]interface{}{
					"topic": "banking.transactions.settled",
					"key":   "{{.payload.transaction_id}}",
					"value": map[string]interface{}{
						"transaction_id": "{{.payload.transaction_id}}",
						"status":         "SETTLED",
					},
				},
				UI: &model.UIMetadata{PositionX: 600, PositionY: 150, Icon: "Layers"},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_ = store.Workflows().Create(ctx, wf2)
}

		if _, err := store.Workflows().Get(ctx, "pix-customer-notifications"); err == storage.ErrWorkflowNotFound {
		paths := []string{
			"examples/05-pix-customer-notifications.yaml",
			"../examples/05-pix-customer-notifications.yaml",
			"../../examples/05-pix-customer-notifications.yaml",
			"d:/DHSA/midaz-orchestrator/examples/05-pix-customer-notifications.yaml",
		}
		for _, p := range paths {
			if data, err := os.ReadFile(p); err == nil {
				var wf4 model.Workflow
				if err := yaml.Unmarshal(data, &wf4); err == nil {
					wf4.CreatedAt = time.Now()
					wf4.UpdatedAt = time.Now()
					_ = store.Workflows().Create(ctx, &wf4)
					break
				}
			}
		}
	}

	if _, err := store.Workflows().Get(ctx, "pix-crossborder-settlement"); err == storage.ErrWorkflowNotFound {
		paths := []string{
			"examples/04-banking-cross-border-settlement.yaml",
			"../examples/04-banking-cross-border-settlement.yaml",
			"../../examples/04-banking-cross-border-settlement.yaml",
			"d:/DHSA/midaz-orchestrator/examples/04-banking-cross-border-settlement.yaml",
		}
		for _, p := range paths {
			if data, err := os.ReadFile(p); err == nil {
				var wf3 model.Workflow
				if err := yaml.Unmarshal(data, &wf3); err == nil {
					wf3.CreatedAt = time.Now()
					wf3.UpdatedAt = time.Now()
					_ = store.Workflows().Create(ctx, &wf3)
					break
				}
			}
		}
	}
}
