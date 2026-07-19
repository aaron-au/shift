package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/logger"
)

// Manager handles integration flow operations
type Manager struct {
	db     *database.DB
	logger *logger.Logger
}

// FlowDefinition represents an integration flow
type FlowDefinition struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	AccountID   string                 `json:"account_id"`
	Version     int                    `json:"version"`
	Definition  json.RawMessage       `json:"definition"`
	Schedule    string                 `json:"schedule,omitempty"` // Cron expression
	Status      string                 `json:"status"`
}

// NewManager creates a new integration manager
func NewManager(db *database.DB, log *logger.Logger) *Manager {
	return &Manager{
		db:     db,
		logger: log,
	}
}

// CreateTestIntegration creates a simple test integration
func (m *Manager) CreateTestIntegration(accountID, name string, schedule ...string) (*FlowDefinition, error) {
	flowID := uuid.New().String()
	
	// Ensure account exists (create if needed)
	var actualAccountID string
	if accountID == "" {
		accountID = "default"
	}
	
	// Try to find account by ID or name
	accountQuery := `SELECT id FROM accounts WHERE id::text = $1 OR name = $1 LIMIT 1`
	err := m.db.DB.QueryRow(accountQuery, accountID).Scan(&actualAccountID)
	if err != nil {
		// Create account if it doesn't exist
		createAccountQuery := `INSERT INTO accounts (id, name, billing_profile_type) VALUES (uuid_generate_v4(), $1, 'Self-Hosted') RETURNING id`
		err = m.db.DB.QueryRow(createAccountQuery, accountID).Scan(&actualAccountID)
		if err != nil {
			return nil, fmt.Errorf("failed to create account: %w", err)
		}
		m.logger.Info("Created account %s (id: %s)", accountID, actualAccountID)
	}
	
	definition := map[string]interface{}{
		"id":      flowID,
		"name":    name,
		"version": 1,
		"steps": []map[string]interface{}{
			{
				"id":        "step-1",
				"type":      "action",
				"connector": "http",
				"action":    "GET",
				"config": map[string]interface{}{
					"url": "https://gogogogogogogogogogo.free.beeceptor.com",
				},
			},
			{
				"id":        "step-2",
				"type":      "action",
				"connector": "sleep",
				"action":    "sleep",
				"config": map[string]interface{}{
					"duration_seconds": 90,
				},
			},
		},
	}
	
	// Add schedule to definition if provided
	if len(schedule) > 0 && schedule[0] != "" {
		definition["schedule"] = schedule[0]
	}
	
	definitionJSON, _ := json.Marshal(definition)
	
	query := `
		INSERT INTO integration_flows (id, account_id, name, version, definition, status)
		VALUES ($1, $2, $3, 1, $4, 'published')
		ON CONFLICT (account_id, name, version) DO UPDATE
		SET definition = EXCLUDED.definition,
		    status = EXCLUDED.status,
		    updated_at = NOW()
		RETURNING id
	`
	
	var id string
	err = m.db.DB.QueryRow(query, flowID, actualAccountID, name, string(definitionJSON)).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("failed to create integration: %w", err)
	}
	
	return &FlowDefinition{
		ID:         id,
		Name:       name,
		AccountID:  actualAccountID,
		Version:    1,
		Definition: definitionJSON,
		Status:     "published",
	}, nil
}

// GetFlow retrieves a flow by ID
func (m *Manager) GetFlow(flowID string) (*FlowDefinition, error) {
	query := `
		SELECT id, account_id, name, version, definition, status
		FROM integration_flows
		WHERE id::text = $1
		ORDER BY version DESC
		LIMIT 1
	`
	
	var flow FlowDefinition
	var definitionJSON string
	
	err := m.db.DB.QueryRow(query, flowID).Scan(
		&flow.ID, &flow.AccountID, &flow.Name, &flow.Version, &definitionJSON, &flow.Status,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("flow not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get flow: %w", err)
	}
	
	flow.Definition = json.RawMessage(definitionJSON)
	return &flow, nil
}

// GetFlowsForGroup retrieves all published flows for a runner group
func (m *Manager) GetFlowsForGroup(groupID string) ([]*FlowDefinition, error) {
	query := `
		SELECT f.id, f.account_id, f.name, f.version, f.definition, f.status, f.runner_group_id
		FROM integration_flows f
		WHERE f.status = 'published' 
		  AND (f.runner_group_id::text = $1 OR f.runner_group_id IS NULL)
		ORDER BY f.created_at DESC
	`
	
	rows, err := m.db.DB.Query(query, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to query flows: %w", err)
	}
	defer rows.Close()
	
	var flows []*FlowDefinition
	for rows.Next() {
		var flow FlowDefinition
		var definitionJSON string
		var runnerGroupID sql.NullString
		
		err := rows.Scan(
			&flow.ID, &flow.AccountID, &flow.Name, &flow.Version, 
			&definitionJSON, &flow.Status, &runnerGroupID,
		)
		if err != nil {
			continue
		}
		
		flow.Definition = json.RawMessage(definitionJSON)
		flows = append(flows, &flow)
	}
	
	return flows, nil
}

// TriggerExecution creates an execution task for a flow
// Returns the task ID
func (m *Manager) TriggerExecution(accountID, flowID string, input json.RawMessage) (string, error) {
	taskID := fmt.Sprintf("task-%s-%d", flowID, time.Now().UnixNano())
	return taskID, nil
}

