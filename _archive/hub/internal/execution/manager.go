package execution

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/logger"
)

// Manager handles integration execution tracking in the Hub
type Manager struct {
	db     *database.DB
	logger *logger.Logger
}

// ExecutionStatus represents the status of an execution
type ExecutionStatus struct {
	TaskID         string
	AccountID      string
	FlowID         string
	RunnerID       string
	Status         string
	DurationMs     int64
	CPUTimeMs      int64    // CPU time used during execution
	MemoryUsedMB   uint64   // Peak memory used during execution
	ConnectorsUsed []string // List of connectors used
	Output         json.RawMessage
	Error          string
	StartedAt      time.Time
	CompletedAt    time.Time
}

// NewManager creates a new execution manager
func NewManager(db *database.DB, log *logger.Logger) *Manager {
	return &Manager{
		db:     db,
		logger: log,
	}
}

// RecordExecutionStatus records an execution status update
func (m *Manager) RecordExecutionStatus(status *ExecutionStatus) error {
	outputJSON, _ := json.Marshal(status.Output)
	
	// Store connectors as JSON array
	connectorsJSON, _ := json.Marshal(status.ConnectorsUsed)
	
	query := `
		INSERT INTO integration_executions (
			id, account_id, flow_id, runner_id, status, 
			output_payload, error_message, started_at, completed_at, duration_ms,
			cpu_time_ms, memory_used_mb, connectors_used
		)
		VALUES ($1, $2, $3, 
			(SELECT id FROM runners WHERE id::text = $4 LIMIT 1),
			$5, $6, $7, $8, $9, $10, $11, $12, $13
		)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status,
		    output_payload = EXCLUDED.output_payload,
		    error_message = EXCLUDED.error_message,
		    started_at = EXCLUDED.started_at,
		    completed_at = EXCLUDED.completed_at,
		    duration_ms = EXCLUDED.duration_ms,
		    cpu_time_ms = EXCLUDED.cpu_time_ms,
		    memory_used_mb = EXCLUDED.memory_used_mb,
		    connectors_used = EXCLUDED.connectors_used,
		    updated_at = NOW()
	`

	_, err := m.db.DB.Exec(query,
		status.TaskID,
		status.AccountID,
		status.FlowID,
		status.RunnerID,
		status.Status,
		string(outputJSON),
		status.Error,
		status.StartedAt,
		status.CompletedAt,
		status.DurationMs,
		status.CPUTimeMs,
		status.MemoryUsedMB,
		string(connectorsJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to record execution status: %w", err)
	}

	m.logger.Info("Recorded execution status: task=%s, status=%s, duration=%dms", 
		status.TaskID, status.Status, status.DurationMs)

	// Record usage metrics for billing
	if status.Status == "completed" {
		m.recordUsageMetric(status)
	}

	return nil
}

// recordUsageMetric records usage metrics for billing
func (m *Manager) recordUsageMetric(status *ExecutionStatus) error {
	query := `
		INSERT INTO usage_metrics (account_id, runner_id, flow_id, metric_type, metric_value, recorded_at)
		VALUES ($1, 
			(SELECT id FROM runners WHERE id::text = $2 LIMIT 1),
			(SELECT id FROM integration_flows WHERE id::text = $3 LIMIT 1),
			'flow_execution', 1, NOW()
		)
	`

	_, err := m.db.DB.Exec(query, status.AccountID, status.RunnerID, status.FlowID)
	if err != nil {
		m.logger.Error("Failed to record usage metric: %v", err)
		return err
	}

	return nil
}

// CreateExecution creates a new execution record
func (m *Manager) CreateExecution(accountID, flowID, taskID string, inputPayload json.RawMessage) error {
	query := `
		INSERT INTO integration_executions (id, account_id, flow_id, status, input_payload)
		VALUES ($1, $2, $3, 'pending', $4)
	`

	inputJSON, _ := json.Marshal(inputPayload)
	_, err := m.db.DB.Exec(query, taskID, accountID, flowID, string(inputJSON))
	if err != nil {
		return fmt.Errorf("failed to create execution: %w", err)
	}

	return nil
}

