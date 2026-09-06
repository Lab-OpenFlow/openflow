package connectors

import (
	"fmt"
	"sync"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// Registry manages all registered connectors.
type Registry struct {
	mu         sync.RWMutex
	connectors map[model.ConnectorType]Connector
}

// NewRegistry creates a new Connector Registry.
func NewRegistry() *Registry {
	return &Registry{
		connectors: make(map[model.ConnectorType]Connector),
	}
}

// Register adds a connector to the registry.
func (r *Registry) Register(c Connector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connectors[c.Type()] = c
}

// Get retrieves a connector by type.
func (r *Registry) Get(cType model.ConnectorType) (Connector, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conn, exists := r.connectors[cType]
	if !exists {
		return nil, fmt.Errorf("connector of type '%s' is not registered", cType)
	}
	return conn, nil
}

// ListDescriptors returns all available connector descriptors for UI/catalog.
func (r *Registry) ListDescriptors() []model.ConnectorDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []model.ConnectorDescriptor
	for _, c := range r.connectors {
		list = append(list, c.Descriptor())
	}
	return list
}
