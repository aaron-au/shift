package connector

import (
	"context"
)

// Connector is the interface that all connector plugins must implement
type Connector interface {
	// Connect initializes the connector with its configuration
	Connect(ctx context.Context, config map[string]interface{}) error
	
	// Execute performs a specific action
	Execute(ctx context.Context, action string, input map[string]interface{}) (map[string]interface{}, error)
	
	// Shutdown gracefully terminates the connector
	Shutdown(ctx context.Context) error
}

// Registry manages available connectors
type Registry struct {
	connectors map[string]Connector
}

// NewRegistry creates a new connector registry
func NewRegistry() *Registry {
	return &Registry{
		connectors: make(map[string]Connector),
	}
}

// Register registers a connector
func (r *Registry) Register(name string, connector Connector) {
	r.connectors[name] = connector
}

// Get retrieves a connector by name
func (r *Registry) Get(name string) (Connector, bool) {
	connector, exists := r.connectors[name]
	return connector, exists
}


