package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the runner
type Config struct {
	// Server configuration
	ServerPort         string
	UIPort             string
	ServerReadTimeout  time.Duration
	ServerWriteTimeout time.Duration

	// Hub WebSocket configuration
	HubWSURL string

	// Hub HTTP API URL (for connector downloads)
	HubAPIURL string

	// P2P WebSocket configuration
	P2PPort string

	// Kafka configuration
	KafkaBrokers string

	// Runner identification
	RunnerID    string
	RunnerName  string
	RunnerGroupID string
}

// Load reads configuration from environment variables
func Load() *Config {
	cfg := &Config{
		ServerPort:         getEnv("SERVER_PORT", "8000"),
		UIPort:             getEnv("UI_PORT", "8001"),
		ServerReadTimeout:  getDurationEnv("SERVER_READ_TIMEOUT", 15*time.Second),
		ServerWriteTimeout: getDurationEnv("SERVER_WRITE_TIMEOUT", 15*time.Second),

		HubWSURL: getEnv("HUB_WS_URL", "ws://localhost:2000/ws"),
		HubAPIURL: getEnv("HUB_API_URL", "http://localhost:2000"),

		P2PPort: getEnv("P2P_PORT", "8002"),

		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),

		RunnerID:      getEnv("RUNNER_ID", ""),
		RunnerName:    getEnv("RUNNER_NAME", "runner-1"),
		RunnerGroupID: getEnv("RUNNER_GROUP_ID", "group-1"),
	}

	return cfg
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

