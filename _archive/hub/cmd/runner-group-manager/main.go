package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shift/hub/internal/config"
	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/logger"
	"github.com/shift/hub/internal/runnergroup"
)

func main() {
	// Initialize logger
	log := logger.New()
	log.Info("Starting Runner Group Manager...")

	// Load configuration
	cfg := config.Load()

	// Initialize database connection
	db, err := database.New(cfg, log)
	if err != nil {
		log.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Initialize runner group manager
	groupManager := runnergroup.NewManager(db, log)

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// API endpoints for group management
	mux.HandleFunc("/api/groups/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Group management endpoints will be implemented here
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte(`{"error": "Not yet implemented"}`))
	})

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      mux,
		ReadTimeout:  cfg.ServerReadTimeout,
		WriteTimeout: cfg.ServerWriteTimeout,
	}

	// Start server in a goroutine
	go func() {
		log.Info("Runner Group Manager listening on port %s", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed to start: %v", err)
		}
	}()

	// Store group manager reference for use by runner-service
	_ = groupManager

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down Runner Group Manager...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("Server forced to shutdown: %v", err)
	}

	log.Info("Runner Group Manager exited")
}

