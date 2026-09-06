# Contributing to OpenFlow

Thank you for your interest in contributing to OpenFlow!

## Development Setup

```bash
git clone https://github.com/Lab-OpenFlow/openflow.git
cd openflow
go mod download

# Run tests
go test -race ./pkg/...

# Build all binaries
make build

# Start local server
make run
```

## Running the Full Stack

```bash
docker compose -f deployments/docker-compose.yml up -d
```

This starts OpenFlow, PostgreSQL, Kafka, RabbitMQ, Prometheus, and Jaeger.

## Project Structure

```
pkg/
  engine/        # Core DAG engine, scheduler, watchdog, DLQ
  connectors/    # Protocol adapters (HTTP, Kafka, gRPC, ...)
  storage/       # PostgreSQL and in-memory stores
  security/      # Vault, JWT, API key auth, RBAC
  observability/ # Prometheus metrics, OpenTelemetry
  api/           # REST API, WebSocket hub
  model/         # Shared domain types
cmd/
  openflow-server/   # Main server binary
  openflowctl/       # CLI tool
  openflow-operator/ # Kubernetes operator
sdk/go/            # Official Go SDK
```

## Pull Request Guidelines

- Keep each PR focused on a single concern.
- Include tests for any new behaviour.
- Run `go test -race ./pkg/...` before submitting.
- Run `go vet ./...` and `go fmt ./...` before submitting.
- Update `CHANGELOG.md` under an `[Unreleased]` section.

## Reporting Issues

Please use the GitHub Issue tracker. For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).
