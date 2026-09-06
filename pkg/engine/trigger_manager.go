package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// TriggerManager coordinates external inbound event listeners.
type TriggerManager struct {
	engine     *Engine
	kafkaSubs  sync.Map // map[workflowID]*kafka.Reader
	rabbitSubs sync.Map // map[workflowID]*amqp.Connection
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewTriggerManager creates a new TriggerManager.
func NewTriggerManager(engine *Engine) *TriggerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TriggerManager{
		engine: engine,
		ctx:    ctx,
		cancel: cancel,
	}
}

// RegisterWorkflowTriggers registers triggers for an active workflow.
// Registration is performed in a background goroutine so broker I/O does not hold any shared lock.
func (tm *TriggerManager) RegisterWorkflowTriggers(wf *model.Workflow) error {
	if wf.Trigger == nil || wf.Status != model.WorkflowStatusActive {
		return nil
	}

	switch wf.Trigger.Type {
	case model.TriggerTypeKafka:
		go tm.startKafkaConsumer(wf)
	case model.TriggerTypeRabbitMQ:
		go tm.startRabbitMQConsumer(wf)
	}

	return nil
}

func (tm *TriggerManager) startKafkaConsumer(wf *model.Workflow) {
	topic := wf.Trigger.Topic
	if topic == "" {
		slog.Warn("kafka trigger missing topic", slog.String("workflow_id", wf.ID))
		return
	}

	broker := wf.Trigger.Broker
	if broker == "" {
		broker = "kafka:29092"
	}

	group := wf.Trigger.Group
	if group == "" {
		group = fmt.Sprintf("openflow_%s", wf.ID)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  group,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	tm.kafkaSubs.Store(wf.ID, reader)
	slog.Info("kafka consumer registered",
		slog.String("workflow_id", wf.ID),
		slog.String("topic", topic),
	)

	for {
		select {
		case <-tm.ctx.Done():
			_ = reader.Close()
			tm.kafkaSubs.Delete(wf.ID)
			return
		default:
			msg, err := reader.ReadMessage(tm.ctx)
			if err != nil {
				if tm.ctx.Err() != nil {
					return
				}
				slog.Error("kafka read error",
					slog.String("topic", topic),
					slog.String("error", err.Error()),
				)
				time.Sleep(2 * time.Second)
				continue
			}

			var payload map[string]interface{}
			if jsonErr := json.Unmarshal(msg.Value, &payload); jsonErr != nil {
				payload = map[string]interface{}{
					"raw": string(msg.Value),
					"key": string(msg.Key),
				}
			}

			// Inject Kafka message key as idempotency key to deduplicate at-least-once delivery.
			// The engine will short-circuit and return the existing execution if already processed.
			if key := string(msg.Key); key != "" {
				payload["__idempotency_key"] = fmt.Sprintf("kafka:%s:%s:%d:%d", topic, key, msg.Partition, msg.Offset)
			}

			slog.Info("kafka trigger received", slog.String("topic", topic), slog.String("workflow_id", wf.ID))
			_, _ = tm.engine.Execute(context.Background(), wf.ID, payload, model.TriggerTypeKafka)
		}
	}
}

func (tm *TriggerManager) startRabbitMQConsumer(wf *model.Workflow) {
	queue := wf.Trigger.Queue
	if queue == "" {
		slog.Warn("rabbitmq trigger missing queue", slog.String("workflow_id", wf.ID))
		return
	}

	url := wf.Trigger.Broker
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		slog.Error("failed to dial rabbitmq",
			slog.String("workflow_id", wf.ID),
			slog.String("error", err.Error()),
		)
		return
	}

	tm.rabbitSubs.Store(wf.ID, conn)
	slog.Info("rabbitmq consumer registered",
		slog.String("workflow_id", wf.ID),
		slog.String("queue", queue),
	)

	defer func() {
		conn.Close()
		tm.rabbitSubs.Delete(wf.ID)
	}()

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("failed to open rabbitmq channel",
			slog.String("workflow_id", wf.ID),
			slog.String("error", err.Error()),
		)
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		slog.Error("failed to declare rabbitmq queue",
			slog.String("workflow_id", wf.ID),
			slog.String("error", err.Error()),
		)
		return
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		slog.Error("failed to start rabbitmq consumer",
			slog.String("workflow_id", wf.ID),
			slog.String("error", err.Error()),
		)
		return
	}

	for {
		select {
		case <-tm.ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			var payload map[string]interface{}
			if jsonErr := json.Unmarshal(d.Body, &payload); jsonErr != nil {
				payload = map[string]interface{}{"raw": string(d.Body)}
			}

			_, _ = tm.engine.Execute(context.Background(), wf.ID, payload, model.TriggerTypeRabbitMQ)
		}
	}
}

// Stop shuts down all background listeners gracefully.
func (tm *TriggerManager) Stop() {
	tm.cancel()

	tm.kafkaSubs.Range(func(key, value interface{}) bool {
		if r, ok := value.(*kafka.Reader); ok {
			_ = r.Close()
		}
		tm.kafkaSubs.Delete(key)
		return true
	})

	tm.rabbitSubs.Range(func(key, value interface{}) bool {
		if c, ok := value.(*amqp.Connection); ok {
			_ = c.Close()
		}
		tm.rabbitSubs.Delete(key)
		return true
	})
}
