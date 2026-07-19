package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"sync"

	"github.com/shift/runner/internal/logger"
)

// Loader manages dynamic connector loading
type Loader struct {
	connectorDir string
	logger       *logger.Logger
	mu           sync.RWMutex
	connectors   map[string]*LoadedConnector // name-version -> connector
}

// LoadedConnector represents a dynamically loaded connector
type LoadedConnector struct {
	Name     string
	Version  string
	Path     string
	Checksum string
	Connector Connector
}

// NewLoader creates a new connector loader
func NewLoader(log *logger.Logger) *Loader {
	connectorDir := filepath.Join(os.TempDir(), "shift-connectors")
	os.MkdirAll(connectorDir, 0755)
	
	return &Loader{
		connectorDir: connectorDir,
		logger:       log,
		connectors:   make(map[string]*LoadedConnector),
	}
}

// RequestConnector requests a connector from the Hub
func (l *Loader) RequestConnector(hubURL, name, version string) (*LoadedConnector, error) {
	// Handle "latest" version by querying Hub for latest version
	if version == "latest" {
		infoURL := fmt.Sprintf("%s/api/connectors/%s/latest", hubURL, name)
		l.logger.Info("Requesting latest connector info from Hub: %s", infoURL)
		resp, err := http.Get(infoURL)
		if err != nil {
			l.logger.Error("Failed to request latest connector info from %s: %v", infoURL, err)
			return nil, fmt.Errorf("failed to request latest connector info: %w", err)
		}
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			l.logger.Error("Connector not found at %s: %s", infoURL, resp.Status)
			return nil, fmt.Errorf("connector not found: %s", resp.Status)
		}
		
		var connInfo struct {
			Name        string `json:"name"`
			Version     string `json:"version"`
			Checksum    string `json:"checksum"`
			DownloadURL string `json:"download_url"`
		}
		
		if err := json.NewDecoder(resp.Body).Decode(&connInfo); err != nil {
			return nil, fmt.Errorf("failed to decode latest connector info: %w", err)
		}
		
		version = connInfo.Version
	}
	
	key := fmt.Sprintf("%s-%s", name, version)
	
	// Check if already loaded (with same checksum to avoid reloading)
	l.mu.RLock()
	if loaded, exists := l.connectors[key]; exists {
		// Verify checksum matches - if it does, reuse the loaded connector
		// This allows hot reloading without restart if checksum changes
		if loaded.Checksum != "" {
			// We'll verify checksum after download, but for now just reuse if loaded
			l.mu.RUnlock()
			l.logger.Info("Connector %s version %s already loaded, reusing", name, version)
			return loaded, nil
		}
	}
	l.mu.RUnlock()
	
	// Request connector info from Hub
	infoURL := fmt.Sprintf("%s/api/connectors/%s/%s", hubURL, name, version)
	l.logger.Info("Requesting connector info from Hub: %s", infoURL)
	resp, err := http.Get(infoURL)
	if err != nil {
		l.logger.Error("Failed to request connector info from %s: %v", infoURL, err)
		return nil, fmt.Errorf("failed to request connector info: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		l.logger.Error("Connector not found at %s: %s", infoURL, resp.Status)
		return nil, fmt.Errorf("connector not found: %s", resp.Status)
	}
	
	var connInfo struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		Checksum    string `json:"checksum"`
		DownloadURL string `json:"download_url"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&connInfo); err != nil {
		return nil, fmt.Errorf("failed to decode connector info: %w", err)
	}
	
	// Download connector binary (must be .so for Go plugins)
	// Use checksum in filename to support multiple versions/updates
	binaryPath := filepath.Join(l.connectorDir, fmt.Sprintf("%s-%s-%s-%s.so", name, connInfo.Version, connInfo.Checksum[:8], runtime.GOOS))
	
	// Check if we already have this exact version (by checksum)
	l.mu.RLock()
	for _, loaded := range l.connectors {
		if loaded.Name == name && loaded.Checksum == connInfo.Checksum {
			l.mu.RUnlock()
			l.logger.Info("Connector %s version %s with checksum %s already loaded, reusing", name, connInfo.Version, connInfo.Checksum[:8])
			return loaded, nil
		}
	}
	l.mu.RUnlock()
	
	l.logger.Info("Downloading connector binary from %s to %s", connInfo.DownloadURL, binaryPath)
	if err := l.downloadConnector(connInfo.DownloadURL, binaryPath, connInfo.Checksum); err != nil {
		return nil, fmt.Errorf("failed to download connector: %w", err)
	}
	
	// Load connector using Go plugin system
	conn, err := l.loadPlugin(binaryPath)
	if err != nil {
		// Clean up failed download
		os.Remove(binaryPath)
		return nil, fmt.Errorf("failed to load plugin: %w", err)
	}
	
	loaded := &LoadedConnector{
		Name:      name,
		Version:   connInfo.Version,
		Path:      binaryPath,
		Checksum:  connInfo.Checksum,
		Connector: conn,
	}
	
	// Store loaded connector (replace if version exists)
	l.mu.Lock()
	// Remove old version if it exists
	if old, exists := l.connectors[key]; exists && old.Checksum != connInfo.Checksum {
		l.logger.Info("Replacing connector %s version %s (old checksum: %s, new: %s)", name, version, old.Checksum[:8], connInfo.Checksum[:8])
	}
	l.connectors[key] = loaded
	l.mu.Unlock()
	
	l.logger.Info("Loaded connector %s version %s from Hub (checksum: %s, path: %s)", name, connInfo.Version, connInfo.Checksum, binaryPath)
	return loaded, nil
}

// downloadConnector downloads a connector binary and verifies checksum
func (l *Loader) downloadConnector(url, path, expectedChecksum string) error {
	// Check if already downloaded and verify checksum
	if _, err := os.Stat(path); err == nil {
		// Verify checksum matches
		if l.verifyChecksum(path, expectedChecksum) {
			l.logger.Info("Connector already downloaded and verified: %s", path)
			return nil
		}
		// Checksum mismatch - remove old file and re-download
		l.logger.Warn("Connector checksum mismatch, re-downloading: %s", path)
		os.Remove(path)
	}
	
	// Download
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: %s", resp.Status)
	}
	
	// Save to file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	
	// Make executable
	os.Chmod(path, 0755)
	
	// Verify checksum
	if !l.verifyChecksum(path, expectedChecksum) {
		return fmt.Errorf("checksum verification failed")
	}
	
	l.logger.Info("Downloaded connector: %s", path)
	return nil
}

// verifyChecksum verifies the SHA256 checksum of a file
func (l *Loader) verifyChecksum(path, expectedChecksum string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	
	hash := sha256.Sum256(data)
	actualChecksum := hex.EncodeToString(hash[:])
	
	return actualChecksum == expectedChecksum
}

// GetConnector retrieves a loaded connector
func (l *Loader) GetConnector(name, version string) (*LoadedConnector, bool) {
	key := fmt.Sprintf("%s-%s", name, version)
	l.mu.RLock()
	defer l.mu.RUnlock()
	conn, exists := l.connectors[key]
	return conn, exists
}

// ListConnectors returns all loaded connectors
func (l *Loader) ListConnectors() []*LoadedConnector {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	connectors := make([]*LoadedConnector, 0, len(l.connectors))
	for _, conn := range l.connectors {
		connectors = append(connectors, conn)
	}
	return connectors
}

// loadPlugin loads a connector plugin from a .so file using Go's plugin system
func (l *Loader) loadPlugin(path string) (Connector, error) {
	// Open the plugin
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin %s: %w", path, err)
	}
	
	// Look up the symbol "Connector" which should be a function that returns a Connector
	sym, err := p.Lookup("Connector")
	if err != nil {
		return nil, fmt.Errorf("failed to find Connector symbol in plugin %s: %w", path, err)
	}
	
	// Type assert to get the connector factory function
	// The plugin should export: func Connector() Connector
	connectorFunc, ok := sym.(func() Connector)
	if !ok {
		// Try direct type assertion if it's already a Connector
		if conn, ok := sym.(Connector); ok {
			return conn, nil
		}
		return nil, fmt.Errorf("Connector symbol is not a function or Connector in plugin %s", path)
	}
	
	// Call the factory function to get the connector instance
	conn := connectorFunc()
	if conn == nil {
		return nil, fmt.Errorf("connector factory returned nil in plugin %s", path)
	}
	
	l.logger.Info("Successfully loaded plugin connector from %s", path)
	return conn, nil
}

