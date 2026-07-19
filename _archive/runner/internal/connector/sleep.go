package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/shift/runner/internal/logger"
)

// SleepConnector implements a simple sleep connector for testing
type SleepConnector struct {
	logger *logger.Logger
}

// NewSleepConnector creates a new sleep connector
func NewSleepConnector(log *logger.Logger) *SleepConnector {
	return &SleepConnector{
		logger: log,
	}
}

// Connect initializes the sleep connector
func (s *SleepConnector) Connect(ctx context.Context, config map[string]interface{}) error {
	s.logger.Info("Sleep connector connected")
	return nil
}

// Execute performs a sleep operation
func (s *SleepConnector) Execute(ctx context.Context, action string, input map[string]interface{}) (map[string]interface{}, error) {
	if action != "sleep" {
		return nil, fmt.Errorf("unknown action: %s", action)
	}
	
	durationSeconds := 90.0 // default
	if dur, ok := input["duration_seconds"].(float64); ok {
		durationSeconds = dur
	}
	
	s.logger.Info("Sleeping for %.0f seconds", durationSeconds)
	
	// Sleep with context cancellation support
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(durationSeconds) * time.Second):
		s.logger.Info("Sleep completed")
		return map[string]interface{}{
			"status":    "completed",
			"duration":  durationSeconds,
			"slept_at":  time.Now().Format(time.RFC3339),
		}, nil
	}
}

// Shutdown gracefully terminates the sleep connector
func (s *SleepConnector) Shutdown(ctx context.Context) error {
	s.logger.Info("Sleep connector shutdown")
	return nil
}


