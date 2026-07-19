package connector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/logger"
)

// Manager handles connector management in the Hub
type Manager struct {
	db          *database.DB
	logger      *logger.Logger
	storagePath string
}

// Connector represents a connector definition
type Connector struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Type        string `json:"type"`
	Description string `json:"description"`
	BinaryURL   string `json:"binary_url,omitempty"`
	Checksum    string `json:"checksum"`
	IsActive    bool   `json:"is_active"`
}

// NewManager creates a new connector manager
func NewManager(db *database.DB, log *logger.Logger, storagePath string) *Manager {
	// Ensure storage directory exists
	if storagePath == "" {
		storagePath = "/tmp/shift-connectors"
	}
	os.MkdirAll(storagePath, 0755)
	
	return &Manager{
		db:          db,
		logger:      log,
		storagePath: storagePath,
	}
}

// RegisterConnector registers a new connector version
func (m *Manager) RegisterConnector(name, version, connectorType, description string, binaryData []byte, checksum string) (*Connector, error) {
	connectorID := uuid.New().String()
	
	// Save binary to storage (use name-version for readability)
	binaryPath := filepath.Join(m.storagePath, fmt.Sprintf("%s-%s", name, version))
	if err := os.WriteFile(binaryPath, binaryData, 0755); err != nil {
		return nil, fmt.Errorf("failed to save connector binary: %w", err)
	}
	
	// Store in database (definition is stored as JSONB)
	definition := map[string]interface{}{
		"name":        name,
		"version":     version,
		"type":        connectorType,
		"description": description,
	}
	definitionJSON, _ := json.Marshal(definition)
	
	query := `
		INSERT INTO connectors (id, name, version, connector_type, definition, binary_url, checksum, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true)
		ON CONFLICT (name, version) DO UPDATE
		SET connector_type = EXCLUDED.connector_type,
		    definition = EXCLUDED.definition,
		    binary_url = EXCLUDED.binary_url,
		    checksum = EXCLUDED.checksum,
		    is_active = EXCLUDED.is_active,
		    updated_at = NOW()
		RETURNING id
	`
	
	var id string
	err := m.db.DB.QueryRow(query, connectorID, name, version, connectorType, string(definitionJSON), binaryPath, checksum).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("failed to register connector: %w", err)
	}
	
	m.logger.Info("Registered connector %s version %s", name, version)
	
	return &Connector{
		ID:          id,
		Name:        name,
		Version:     version,
		Type:        connectorType,
		Description: description,
		BinaryURL:   binaryPath,
		Checksum:    checksum,
		IsActive:    true,
	}, nil
}

// GetConnector retrieves a connector by name and version
func (m *Manager) GetConnector(name, version string) (*Connector, error) {
	query := `
		SELECT id, name, version, connector_type, definition, binary_url, checksum, is_active
		FROM connectors
		WHERE name = $1 AND version = $2 AND is_active = true
	`
	
	var conn Connector
	var definitionJSON string
	err := m.db.DB.QueryRow(query, name, version).Scan(
		&conn.ID, &conn.Name, &conn.Version, &conn.Type, &definitionJSON,
		&conn.BinaryURL, &conn.Checksum, &conn.IsActive,
	)
	if err == nil {
		// Parse description from definition
		var definition map[string]interface{}
		if err := json.Unmarshal([]byte(definitionJSON), &definition); err == nil {
			if desc, ok := definition["description"].(string); ok {
				conn.Description = desc
			}
		}
	}
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connector %s version %s not found", name, version)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	
	return &conn, nil
}

// GetLatestConnector retrieves the latest version of a connector
func (m *Manager) GetLatestConnector(name string) (*Connector, error) {
	query := `
		SELECT id, name, version, connector_type, definition, binary_url, checksum, is_active
		FROM connectors
		WHERE name = $1 AND is_active = true
		ORDER BY version DESC
		LIMIT 1
	`
	
	var conn Connector
	var definitionJSON string
	err := m.db.DB.QueryRow(query, name).Scan(
		&conn.ID, &conn.Name, &conn.Version, &conn.Type, &definitionJSON,
		&conn.BinaryURL, &conn.Checksum, &conn.IsActive,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connector %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	
	// Parse description from definition
	var definition map[string]interface{}
	if err := json.Unmarshal([]byte(definitionJSON), &definition); err == nil {
		if desc, ok := definition["description"].(string); ok {
			conn.Description = desc
		}
	}
	
	return &conn, nil
}

// GetConnectorBinary retrieves the binary data for a connector
func (m *Manager) GetConnectorBinary(name, version string) ([]byte, error) {
	conn, err := m.GetConnector(name, version)
	if err != nil {
		return nil, err
	}
	
	// Read binary file
	binaryData, err := os.ReadFile(conn.BinaryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to read connector binary: %w", err)
	}
	
	return binaryData, nil
}

// ServeConnectorBinary serves the connector binary as a download
func (m *Manager) ServeConnectorBinary(w io.Writer, name, version string) error {
	binaryData, err := m.GetConnectorBinary(name, version)
	if err != nil {
		return err
	}
	
	_, err = w.Write(binaryData)
	return err
}

