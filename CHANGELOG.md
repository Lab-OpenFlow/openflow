# CHANGELOG

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-09-06

### Added
- Core DAG/BPMN workflow engine with goroutine-based stage dispatch.
- Pluggable connector registry: HTTP, Kafka, RabbitMQ, gRPC, WebSocket, SQL, Transform, WASM.
- Transactional Outbox Pattern for reliable at-least-once event delivery.
- Deterministic idempotency key injection into all connectors (SHA-256 per execution+stage+attempt).
- Dead Letter Queue (DLQ) with configurable retry backoff.
- Saga rollback coordinator (LIFO compensation chain).
- Distributed leader election via PostgreSQL advisory locks.
- Consistent hash ring for cluster-aware execution routing.
- Durable timer scheduler for `sleep` and `delay` stage types.
- Recovery watchdog for at-most-once crash recovery.
- Rotating Bloom Filter for fast in-memory idempotency deduplication.
- Rolling Merkle Tree for incremental audit integrity.
- AES-GCM 256-bit secret vault with PostgreSQL-backed persistence.
- JWT and API key authentication with RBAC (admin, operator, viewer).
- Rate limiting middleware per API key.
- Prometheus metrics and OpenTelemetry tracing (OTLP/Jaeger).
- Structured logging throughout using `log/slog`.
- Embedded Visual Studio UI (React + Vite + TypeScript, drag-and-drop canvas).
- REST API: workflows, executions, signals, approvals, DLQ, task queues, secrets, webhooks.
- WebSocket live execution event streaming.
- `openflowctl` CLI: workflow apply, list, run, inspect, signals, approvals, versioning.
- Kubernetes Operator (CRD-based `WorkflowController`).
- Go SDK (`github.com/Lab-OpenFlow/openflow/sdk/go`).
- Docker Compose, Helm chart, Kubernetes manifests.
- Example workflows: Kafka pipeline, financial Saga, e-commerce fulfillment.

### Security
- Removed all hardcoded default credentials. Passwords and encryption keys are now randomly generated on startup if environment variables are not set, with warnings logged.
- Removed domain-specific fake compliance strings from codebase.

[0.1.0]: https://github.com/Lab-OpenFlow/openflow/releases/tag/v0.1.0
