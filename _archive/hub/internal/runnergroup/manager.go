package runnergroup

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/logger"
)

// RunnerGroupConfig represents the configuration for a runner group
type RunnerGroupConfig struct {
	GroupID      string                 `json:"group_id"`
	Members      []GroupMember          `json:"members"`
	GroupSecret  string                 `json:"group_secret"` // Pre-shared key for P2P auth
	WebSocketURL string                 `json:"websocket_url"`
	Version      int                    `json:"version"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// GroupMember represents a member of a runner group
type GroupMember struct {
	RunnerID   string `json:"runner_id"`
	Name       string `json:"name"`
	Hostname   string `json:"hostname"`
	P2PPort    int    `json:"p2p_port"`
	Status     string `json:"status"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
}

// RunnerGroupStatus represents the status of a runner group
type RunnerGroupStatus struct {
	GroupID         string                 `json:"group_id"`
	HealthScore     int                    `json:"health_score"`
	MemberCount     int                    `json:"member_count"`
	OnlineMembers   int                    `json:"online_members"`
	ReportedBy      string                 `json:"reported_by"`
	MemberStatuses map[string]interface{} `json:"member_statuses"`
	RecordedAt      time.Time              `json:"recorded_at"`
}

// Manager handles runner group operations
type Manager struct {
	db     *database.DB
	logger *logger.Logger
}

// NewManager creates a new runner group manager
func NewManager(db *database.DB, log *logger.Logger) *Manager {
	return &Manager{
		db:     db,
		logger: log,
	}
}

// GenerateGroupSecret generates a secure random secret for a runner group
func GenerateGroupSecret() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// HashGroupSecret hashes a group secret for storage
func HashGroupSecret(secret string) string {
	hash := sha256.Sum256([]byte(secret))
	return base64.URLEncoding.EncodeToString(hash[:])
}

// GetGroupConfig retrieves the current configuration for a runner group
func (m *Manager) GetGroupConfig(groupID string) (*RunnerGroupConfig, error) {
	query := `
		SELECT rgc.config, rgc.group_secret_hash, rgc.version
		FROM runner_group_configs rgc
		JOIN runner_groups rg ON rgc.runner_group_id = rg.id
		WHERE rg.id::text = $1 OR rg.name = $1
		ORDER BY rgc.version DESC
		LIMIT 1
	`

	var configJSON []byte
	var secretHash string
	var version int

	err := m.db.QueryRow(query, groupID).Scan(&configJSON, &secretHash, &version)
	if err != nil {
		return nil, fmt.Errorf("failed to get group config: %w", err)
	}

	var config RunnerGroupConfig
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	config.Version = version
	// Note: We don't return the actual secret hash for security

	return &config, nil
}

// UpdateGroupConfig updates the configuration for a runner group
func (m *Manager) UpdateGroupConfig(groupID string, config *RunnerGroupConfig) error {
	// Generate new secret if not provided
	if config.GroupSecret == "" {
		secret, err := GenerateGroupSecret()
		if err != nil {
			return fmt.Errorf("failed to generate group secret: %w", err)
		}
		config.GroupSecret = secret
	}

	secretHash := HashGroupSecret(config.GroupSecret)

	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Get the latest version
	var currentVersion int
	versionQuery := `
		SELECT COALESCE(MAX(version), 0)
		FROM runner_group_configs rgc
		JOIN runner_groups rg ON rgc.runner_group_id = rg.id
		WHERE rg.id::text = $1 OR rg.name = $1
	`
	m.db.QueryRow(versionQuery, groupID).Scan(&currentVersion)

	newVersion := currentVersion + 1
	config.Version = newVersion

	insertQuery := `
		INSERT INTO runner_group_configs (runner_group_id, config, group_secret_hash, version)
		SELECT rg.id, $1, $2, $3
		FROM runner_groups rg
		WHERE rg.id::text = $4 OR rg.name = $4
		RETURNING id
	`

	var configID string
	err = m.db.QueryRow(insertQuery, configJSON, secretHash, newVersion, groupID).Scan(&configID)
	if err != nil {
		return fmt.Errorf("failed to insert group config: %w", err)
	}

	m.logger.Info("Updated group config for group %s, version %d", groupID, newVersion)
	return nil
}

// GetGroupMembers retrieves all members of a runner group
func (m *Manager) GetGroupMembers(groupID string) ([]GroupMember, error) {
	query := `
		SELECT r.id, r.name, r.hostname, r.p2p_port, r.status, r.last_seen_at
		FROM runners r
		JOIN runner_groups rg ON r.runner_group_id = rg.id
		WHERE rg.id::text = $1 OR rg.name = $1
		ORDER BY r.created_at
	`

	rows, err := m.db.Query(query, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to query group members: %w", err)
	}
	defer rows.Close()

	var members []GroupMember
	for rows.Next() {
		var m GroupMember
		var lastSeenAt *time.Time
		err := rows.Scan(&m.RunnerID, &m.Name, &m.Hostname, &m.P2PPort, &m.Status, &lastSeenAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan member: %w", err)
		}
		m.LastSeenAt = lastSeenAt
		members = append(members, m)
	}

	return members, nil
}

// CreateOrGetGroup creates a runner group if it doesn't exist, or returns existing one
func (m *Manager) CreateOrGetGroup(accountID, groupID string) (string, error) {
	// Try to find existing group by ID or name
	var existingID string
	query := `SELECT id FROM runner_groups WHERE id::text = $1 OR name = $1 LIMIT 1`
	err := m.db.QueryRow(query, groupID).Scan(&existingID)
	if err == nil {
		return existingID, nil
	}

	// Group doesn't exist, create it
	// If accountID is empty, use a default account or create one
	if accountID == "" {
		// Create default account if needed
		var defaultAccountID string
		accountQuery := `SELECT id FROM accounts WHERE name = 'default' LIMIT 1`
		err := m.db.QueryRow(accountQuery).Scan(&defaultAccountID)
		if err != nil {
			// Create default account
			createAccountQuery := `INSERT INTO accounts (id, name, billing_profile_type) VALUES (uuid_generate_v4(), 'default', 'Self-Hosted') RETURNING id`
			err = m.db.QueryRow(createAccountQuery).Scan(&defaultAccountID)
			if err != nil {
				return "", fmt.Errorf("failed to create default account: %w", err)
			}
		}
		accountID = defaultAccountID
	}

	// Create runner group
	createGroupQuery := `
		INSERT INTO runner_groups (id, account_id, name)
		VALUES (uuid_generate_v4(), $1, $2)
		RETURNING id
	`
	var newGroupID string
	err = m.db.QueryRow(createGroupQuery, accountID, groupID).Scan(&newGroupID)
	if err != nil {
		return "", fmt.Errorf("failed to create runner group: %w", err)
	}

	m.logger.Info("Created runner group %s (id: %s)", groupID, newGroupID)
	return newGroupID, nil
}

// RegisterRunner registers a new runner and assigns it to a group
func (m *Manager) RegisterRunner(runnerID, runnerName, groupID, hostname string, p2pPort int) error {
	// Ensure group exists
	actualGroupID, err := m.CreateOrGetGroup("", groupID)
	if err != nil {
		return fmt.Errorf("failed to ensure group exists: %w", err)
	}

	query := `
		INSERT INTO runners (runner_group_id, name, registration_token, hostname, p2p_port, status)
		VALUES ($1, $2, $3, $4, $5, 'online')
		ON CONFLICT (registration_token) DO UPDATE
		SET name = EXCLUDED.name,
		    hostname = EXCLUDED.hostname,
		    p2p_port = EXCLUDED.p2p_port,
		    status = 'online',
		    last_seen_at = NOW(),
		    updated_at = NOW()
		RETURNING id
	`

	var id string
	err = m.db.QueryRow(query, actualGroupID, runnerName, runnerID, hostname, p2pPort).Scan(&id)
	if err != nil {
		return fmt.Errorf("failed to register runner: %w", err)
	}

	m.logger.Info("Registered runner %s to group %s", runnerID, groupID)

	// Update group config with new member
	config, err := m.GetGroupConfig(groupID)
	if err != nil {
		// Create new config if it doesn't exist
		members, _ := m.GetGroupMembers(groupID)
		config = &RunnerGroupConfig{
			GroupID:     groupID,
			Members:     members,
			WebSocketURL: "ws://runner-service:2000/ws",
		}
	} else {
		// Update existing config
		config.Members, _ = m.GetGroupMembers(groupID)
	}

	return m.UpdateGroupConfig(groupID, config)
}

// RecordGroupStatus records the status of a runner group
func (m *Manager) RecordGroupStatus(status *RunnerGroupStatus) error {
	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal status: %w", err)
	}

	query := `
		INSERT INTO runner_group_status (runner_group_id, reported_by_runner_id, status, health_score)
		SELECT rg.id, r.id, $1, $2
		FROM runner_groups rg
		LEFT JOIN runners r ON r.id::text = $3
		WHERE rg.id::text = $4 OR rg.name = $4
		RETURNING id
	`

	var statusID string
	err = m.db.QueryRow(query, statusJSON, status.HealthScore, status.ReportedBy, status.GroupID).Scan(&statusID)
	if err != nil {
		return fmt.Errorf("failed to record group status: %w", err)
	}

	return nil
}

