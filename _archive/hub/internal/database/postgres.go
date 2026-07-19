package database

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shift/hub/internal/config"
	"github.com/shift/hub/internal/logger"
)

// DB wraps the database connection
type DB struct {
	*sql.DB
	logger *logger.Logger
}

// New creates a new database connection
func New(cfg *config.Config, log *logger.Logger) (*DB, error) {
	dsn := cfg.DatabaseDSN()
	
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Info("Successfully connected to PostgreSQL database")

	return &DB{
		DB:     db,
		logger: log,
	}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	if db.DB != nil {
		db.logger.Info("Closing database connection")
		return db.DB.Close()
	}
	return nil
}

