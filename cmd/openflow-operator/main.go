package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/k8s"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func main() {
	slog.Info("starting openflow kubernetes operator", slog.String("version", "1.0.0"))

	var store storage.Store
	dsn := os.Getenv("DB_DSN")
	if dsn != "" {
		pgStore, err := storage.NewPostgresStore(dsn)
		if err != nil {
			slog.Error("failed to initialize storage", slog.String("error", err.Error()))
			os.Exit(1)
		}
		store = pgStore
	} else {
		store = storage.NewMemoryStore()
		slog.Info("running with in-memory storage")
	}

	reg := connectors.NewRegistry()
	reg.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, reg)
	controller := k8s.NewWorkflowController(eng, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = ctx
	_ = controller
	slog.Info("kubernetes reconciler initialized")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down operator")
}
