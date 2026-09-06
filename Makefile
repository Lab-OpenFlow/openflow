.PHONY: all build test run clean lint

# Default target
all: test build

# Build Go binaries (Server, CLI, Operator)
build:
	@echo "Building OpenFlow Server, CLI, and Operator..."
	go build -o bin/openflow-server ./cmd/openflow-server
	go build -o bin/openflowctl ./cmd/openflowctl
	go build -o bin/openflow-operator ./cmd/openflow-operator
	@echo "Build complete: bin/openflow-server, bin/openflowctl, bin/openflow-operator"

# Run all unit and integration tests
test:
	@echo "Running Go test suite..."
	go test -v -race ./pkg/...

# Run OpenFlow server locally
run: build
	@echo "Starting OpenFlow server on :8080..."
	./bin/openflow-server

# Vet and lint Go code
lint:
	@echo "Running go vet..."
	go vet ./...

# Clean build artifacts
clean:
	rm -rf bin/

