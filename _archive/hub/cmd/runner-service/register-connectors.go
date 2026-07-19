package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/shift/hub/internal/connector"
	"github.com/shift/hub/internal/logger"
)

// registerConnectorsFromFilesystem registers connectors from the filesystem
// This is called on startup to load connector plugins
func registerConnectorsFromFilesystem(connectorManager *connector.Manager, log *logger.Logger, connectorsDir string) error {
	if connectorsDir == "" {
		connectorsDir = "./connectors"
	}

	// Check if connectors directory exists
	if _, err := os.Stat(connectorsDir); os.IsNotExist(err) {
		log.Warn("Connectors directory %s does not exist, skipping connector registration", connectorsDir)
		return nil
	}

	// Register HTTP connector
	httpPath := filepath.Join(connectorsDir, "http-1.0.0-linux.so")
	if _, err := os.Stat(httpPath); err == nil {
		binaryData, err := os.ReadFile(httpPath)
		if err != nil {
			return fmt.Errorf("failed to read HTTP connector: %w", err)
		}
		checksum := calculateChecksum(binaryData)
		_, err = connectorManager.RegisterConnector("http", "1.0.0", "HTTP", "HTTP connector for REST APIs", binaryData, checksum)
		if err != nil {
			return fmt.Errorf("failed to register HTTP connector: %w", err)
		}
		log.Info("Registered HTTP connector from %s (checksum: %s)", httpPath, checksum)
	} else {
		log.Warn("HTTP connector file not found at %s", httpPath)
	}

	// Register Sleep connector
	sleepPath := filepath.Join(connectorsDir, "sleep-1.0.0-linux.so")
	if _, err := os.Stat(sleepPath); err == nil {
		binaryData, err := os.ReadFile(sleepPath)
		if err != nil {
			return fmt.Errorf("failed to read Sleep connector: %w", err)
		}
		checksum := calculateChecksum(binaryData)
		_, err = connectorManager.RegisterConnector("sleep", "1.0.0", "Custom", "Sleep connector for testing", binaryData, checksum)
		if err != nil {
			return fmt.Errorf("failed to register Sleep connector: %w", err)
		}
		log.Info("Registered Sleep connector from %s (checksum: %s)", sleepPath, checksum)
	} else {
		log.Warn("Sleep connector file not found at %s", sleepPath)
	}

	return nil
}

func calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}


