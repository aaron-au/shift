package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the application
type Config struct {
	// Server configuration
	ServerPort         string
	ServerReadTimeout  time.Duration
	ServerWriteTimeout time.Duration

	// Database configuration
	DatabaseHost     string
	DatabasePort     string
	DatabaseUser     string
	DatabasePassword string
	DatabaseName     string
	DatabaseSSLMode  string

	// OAuth/OIDC configuration
	OIDCIssuerURL   string
	OIDCClientID    string
	OIDCClientSecret string

	// WebSocket configuration
	WebSocketReadBufferSize  int
	WebSocketWriteBufferSize int
	WebSocketPingPeriod      time.Duration
}

// Load reads configuration from environment variables
func Load() *Config {
	cfg := &Config{
		ServerPort:         getEnv("SERVER_PORT", "8080"),
		ServerReadTimeout:  getDurationEnv("SERVER_READ_TIMEOUT", 15*time.Second),
		ServerWriteTimeout: getDurationEnv("SERVER_WRITE_TIMEOUT", 15*time.Second),

		DatabaseHost:     getEnv("DB_HOST", "localhost"),
		DatabasePort:     getEnv("DB_PORT", "5432"),
		DatabaseUser:     getEnv("DB_USER", "shift"),
		DatabasePassword: getEnv("DB_PASSWORD", ""),
		DatabaseName:     getEnv("DB_NAME", "shift"),
		DatabaseSSLMode:  getEnv("DB_SSLMODE", "disable"),

		OIDCIssuerURL:    getEnv("OIDC_ISSUER_URL", ""),
		OIDCClientID:     getEnv("OIDC_CLIENT_ID", ""),
		OIDCClientSecret: getEnv("OIDC_CLIENT_SECRET", ""),

		WebSocketReadBufferSize:  getIntEnv("WS_READ_BUFFER_SIZE", 1024),
		WebSocketWriteBufferSize: getIntEnv("WS_WRITE_BUFFER_SIZE", 1024),
		WebSocketPingPeriod:      getDurationEnv("WS_PING_PERIOD", 54*time.Second),
	}

	return cfg
}

// DatabaseDSN returns the PostgreSQL connection string
func (c *Config) DatabaseDSN() string {
	return "host=" + c.DatabaseHost +
		" port=" + c.DatabasePort +
		" user=" + c.DatabaseUser +
		" password=" + c.DatabasePassword +
		" dbname=" + c.DatabaseName +
		" sslmode=" + c.DatabaseSSLMode
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

