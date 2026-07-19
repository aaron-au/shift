package runnergroup

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/shift/runner/internal/logger"
)

// GroupConfig represents the runner group configuration received from Hub
type GroupConfig struct {
	GroupID      string       `json:"group_id"`
	Members      []Member     `json:"members"`
	GroupSecret  string       `json:"group_secret"`
	WebSocketURL string       `json:"websocket_url"`
	Version      int          `json:"version"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// Member represents a member of the runner group
type Member struct {
	RunnerID   string     `json:"runner_id"`
	Name       string     `json:"name"`
	Hostname   string     `json:"hostname"`
	P2PPort    int        `json:"p2p_port"`
	Status     string     `json:"status"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
}

// Manager manages runner group configuration and peer connections
type Manager struct {
	config     *GroupConfig
	configLock sync.RWMutex
	logger     *logger.Logger
	onConfigUpdate func(*GroupConfig)
}

// NewManager creates a new runner group manager
func NewManager(log *logger.Logger) *Manager {
	return &Manager{
		logger: log,
	}
}

// SetConfigUpdateCallback sets a callback for when group config is updated
func (m *Manager) SetConfigUpdateCallback(callback func(*GroupConfig)) {
	m.onConfigUpdate = callback
}

// UpdateConfig updates the group configuration
func (m *Manager) UpdateConfig(configJSON []byte) error {
	var config GroupConfig
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return fmt.Errorf("failed to unmarshal group config: %w", err)
	}

	m.configLock.Lock()
	oldVersion := 0
	if m.config != nil {
		oldVersion = m.config.Version
	}
	m.config = &config
	m.configLock.Unlock()

	m.logger.Info("Updated group config: group=%s, version=%d (was %d), members=%d",
		config.GroupID, config.Version, oldVersion, len(config.Members))

	if m.onConfigUpdate != nil {
		m.onConfigUpdate(&config)
	}

	return nil
}

// GetConfig returns the current group configuration
func (m *Manager) GetConfig() *GroupConfig {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	return m.config
}

// GetPeerEndpoints returns the P2P WebSocket endpoints for all peers
func (m *Manager) GetPeerEndpoints(excludeRunnerID string) []string {
	m.configLock.RLock()
	defer m.configLock.RUnlock()

	if m.config == nil {
		return []string{}
	}

	var endpoints []string
	for _, member := range m.config.Members {
		if member.RunnerID != excludeRunnerID && member.Status == "online" {
			// Construct WebSocket URL for peer
			scheme := "ws"
			host := member.Hostname
			if host == "" {
				continue
			}
			port := member.P2PPort
			if port == 0 {
				port = 8002 // Default P2P port
			}
			endpoint := fmt.Sprintf("%s://%s:%d/p2p?peer_id=%s", scheme, host, port, member.RunnerID)
			endpoints = append(endpoints, endpoint)
		}
	}

	return endpoints
}

// GetGroupSecret returns the group secret for P2P authentication
func (m *Manager) GetGroupSecret() string {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	if m.config == nil {
		return ""
	}
	return m.config.GroupSecret
}

// IsMember checks if a runner ID is a member of the group
func (m *Manager) IsMember(runnerID string) bool {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	if m.config == nil {
		return false
	}
	for _, member := range m.config.Members {
		if member.RunnerID == runnerID {
			return true
		}
	}
	return false
}

