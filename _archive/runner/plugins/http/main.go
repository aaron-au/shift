package main

import (
	"context"
	"log"
	"os"

	"github.com/shift/runner/internal/connector"
	"github.com/shift/runner/internal/logger"
)

// Connector is the exported symbol that the plugin loader will look for
// It returns a factory function that creates a new connector instance
var Connector = func() connector.Connector {
	// Create a logger for the plugin
	// In a plugin, we can't easily share the runner's logger, so we create a simple one
	logger := logger.New()
	return connector.NewHTTPConnector(logger)
}

// This is required for Go plugins - main() must exist but can be empty
func main() {
	// Plugins don't run as standalone programs
	// This is just to satisfy the plugin build requirements
	if len(os.Args) > 0 && os.Args[0] == "test" {
		// Test mode - verify the connector works
		conn := Connector()
		if conn == nil {
			log.Fatal("Connector factory returned nil")
		}
		ctx := context.Background()
		if err := conn.Connect(ctx, map[string]interface{}{}); err != nil {
			log.Fatalf("Connector connect failed: %v", err)
		}
		log.Println("HTTP connector plugin test passed")
	}
}


