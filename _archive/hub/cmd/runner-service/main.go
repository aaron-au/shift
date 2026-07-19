package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shift/hub/internal/config"
	"github.com/shift/hub/internal/connector"
	"github.com/shift/hub/internal/database"
	"github.com/shift/hub/internal/docker"
	"github.com/shift/hub/internal/execution"
	"github.com/shift/hub/internal/integration"
	"github.com/shift/hub/internal/logger"
	"github.com/shift/hub/internal/runnergroup"
	"github.com/shift/hub/internal/websocket"
)

func main() {
	// Initialize logger
	log := logger.New()
	log.Info("Starting Runner Service...")

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

	// Initialize execution manager
	execManager := execution.NewManager(db, log)

	// Initialize integration manager
	integrationManager := integration.NewManager(db, log)

	// Initialize Docker client for log streaming
	dockerClient, err := docker.NewClient(log)
	if err != nil {
		log.Warn("Failed to initialize Docker client (logs streaming unavailable): %v", err)
		dockerClient = nil
	}

	// Initialize connector manager
	connectorManager := connector.NewManager(db, log, "/tmp/shift-connectors")
	
	// Register connectors from filesystem (connector plugins)
	// Connectors should be built and placed in ./connectors directory
	connectorsDir := os.Getenv("CONNECTORS_DIR")
	if connectorsDir == "" {
		connectorsDir = "./connectors"
	}
	if err := registerConnectorsFromFilesystem(connectorManager, log, connectorsDir); err != nil {
		log.Error("Failed to register connectors from filesystem: %v", err)
		// Continue anyway - connectors can be registered via API later
	}

	// Initialize WebSocket hub with execution manager callback
	wsHub := websocket.NewHub(log)
	wsHub.SetMessageHandler(func(msg websocket.Message) {
		// Hub only tracks online/offline status, not detailed resource metrics
		// Runners coordinate resource usage among themselves via P2P
		
		// Handle ExecutionStatus messages from runners
		if msg.Type == "ExecutionStatus" {
			var payload map[string]interface{}
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				log.Error("Failed to unmarshal execution status payload: %v", err)
				return
			}
			
			status := execution.ExecutionStatus{
				RunnerID: msg.From,
			}
			
			// Extract fields from payload
			if taskID, ok := payload["task_id"].(string); ok {
				status.TaskID = taskID
			}
			if accountID, ok := payload["account_id"].(string); ok {
				status.AccountID = accountID
			}
			if flowID, ok := payload["flow_id"].(string); ok {
				status.FlowID = flowID
			}
			if statusStr, ok := payload["status"].(string); ok {
				status.Status = statusStr
			}
			if duration, ok := payload["duration_ms"].(float64); ok {
				status.DurationMs = int64(duration)
			}
			if cpuTime, ok := payload["cpu_time_ms"].(float64); ok {
				status.CPUTimeMs = int64(cpuTime)
			}
			if memoryUsed, ok := payload["memory_used_mb"].(float64); ok {
				status.MemoryUsedMB = uint64(memoryUsed)
			}
			if connectors, ok := payload["connectors_used"].([]interface{}); ok {
				status.ConnectorsUsed = make([]string, 0, len(connectors))
				for _, c := range connectors {
					if connector, ok := c.(string); ok {
						status.ConnectorsUsed = append(status.ConnectorsUsed, connector)
					}
				}
			}
			if output, ok := payload["output"]; ok {
				outputJSON, _ := json.Marshal(output)
				status.Output = outputJSON
			}
			if errStr, ok := payload["error"].(string); ok {
				status.Error = errStr
			}
			
			// Parse timestamps
			if startedAtStr, ok := payload["started_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, startedAtStr); err == nil {
					status.StartedAt = t
				}
			}
			if completedAtStr, ok := payload["completed_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, completedAtStr); err == nil {
					status.CompletedAt = t
				}
			}
			
			if err := execManager.RecordExecutionStatus(&status); err != nil {
				log.Error("Failed to record execution status: %v", err)
			}
		}
	})
	go wsHub.Run()

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// Get runner ID from query parameter or header
		runnerID := r.URL.Query().Get("runner_id")
		if runnerID == "" {
			runnerID = r.Header.Get("X-Runner-ID")
		}
		if runnerID == "" {
			runnerID = "unknown-" + time.Now().Format("20060102150405")
		}
		
		// Check if this is a UI connection (not a runner)
		isRunner := r.URL.Query().Get("type") != "ui"
		
		if isRunner {
			// Register runner and send group config
			groupID := r.URL.Query().Get("group_id")
			if groupID == "" {
				groupID = "group-1" // Default group
			}
			
			hostname := r.Host
			if hostname == "" {
				hostname = r.RemoteAddr
			}
			
			// Extract P2P port from query or use default
			p2pPort := 8002 // Default
			if p2pPortStr := r.URL.Query().Get("p2p_port"); p2pPortStr != "" {
				fmt.Sscanf(p2pPortStr, "%d", &p2pPort)
			}
			
			// Register runner
			if err := groupManager.RegisterRunner(runnerID, runnerID, groupID, hostname, p2pPort); err != nil {
				log.Error("Failed to register runner: %v", err)
			}
			
			// Get group config and send to runner, then send all deployed integrations
			go func() {
				time.Sleep(100 * time.Millisecond) // Small delay to ensure connection is established
				config, err := groupManager.GetGroupConfig(groupID)
				if err == nil {
					configJSON, _ := json.Marshal(config)
					msg := websocket.Message{
						Type:      "GroupConfig",
						Payload:   configJSON,
						Timestamp: time.Now(),
						From:      "hub",
						To:        runnerID,
					}
					wsHub.SendToRunner(runnerID, msg)
					
					// Also send time sync information
					timeSyncMsg := websocket.Message{
						Type:      "TimeSync",
						Timestamp: time.Now(),
						From:      "hub",
						To:        runnerID,
					}
					timeSyncPayload := map[string]interface{}{
						"server_time": time.Now().Unix(),
						"timezone":    "UTC", // Default to UTC, can be configured
					}
					timeSyncJSON, _ := json.Marshal(timeSyncPayload)
					timeSyncMsg.Payload = timeSyncJSON
					wsHub.SendToRunner(runnerID, timeSyncMsg)
					
					// Send all deployed integrations for this group
					flows, err := integrationManager.GetFlowsForGroup(groupID)
					if err == nil && len(flows) > 0 {
						log.Info("Sending %d deployed integrations to runner %s", len(flows), runnerID)
						for _, flow := range flows {
							// Extract schedule from definition if present
							var schedule string
							var flowDef map[string]interface{}
							if err := json.Unmarshal(flow.Definition, &flowDef); err == nil {
								if s, ok := flowDef["schedule"].(string); ok {
									schedule = s
								}
							}
							
							deployMsg := websocket.Message{
								Type:      "DeployFlow",
								Timestamp: time.Now(),
								From:      "hub",
								To:        runnerID,
							}
							
							payload := map[string]interface{}{
								"flowId":     flow.ID,
								"version":    flow.Version,
								"accountId":  flow.AccountID,
								"definition": flow.Definition,
							}
							if schedule != "" {
								payload["schedule"] = schedule
							}
							
							payloadJSON, _ := json.Marshal(payload)
							deployMsg.Payload = payloadJSON
							wsHub.SendToRunner(runnerID, deployMsg)
							
							// Small delay between messages to avoid overwhelming the runner
							time.Sleep(50 * time.Millisecond)
						}
						log.Info("Finished sending %d integrations to runner %s", len(flows), runnerID)
					}
				}
			}()
		}
		
		wsHub.HandleWebSocket(w, r, runnerID, isRunner)
	})
	
	// API endpoints for triggering integrations
	mux.HandleFunc("/api/integrations/create-test", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Received POST /api/integrations/create-test")
		fmt.Printf("[DEBUG] Received POST /api/integrations/create-test\n")
		
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		
		var req struct {
			AccountID string `json:"accountId"`
			Name      string `json:"name"`
			Schedule  string `json:"schedule,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Error("Failed to decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		
		log.Info("Creating integration: accountId=%s, name=%s, schedule=%s", req.AccountID, req.Name, req.Schedule)
		fmt.Printf("[DEBUG] Creating integration: accountId=%s, name=%s, schedule=%s\n", req.AccountID, req.Name, req.Schedule)
		
		flow, err := integrationManager.CreateTestIntegration(req.AccountID, req.Name, req.Schedule)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		
		log.Info("Created integration flow %s, deploying to runners...", flow.ID)
		fmt.Printf("[DEBUG] Created integration flow %s, deploying to runners...\n", flow.ID)
		
		// Deploy flow to all runners in the default group
		config, err := groupManager.GetGroupConfig("group-1")
		log.Info("GetGroupConfig result: err=%v, config=%v", err, config != nil)
		fmt.Printf("[DEBUG] GetGroupConfig result: err=%v, config=%v\n", err, config != nil)
		if err != nil {
			log.Error("Failed to get group config: %v", err)
			// Try to get members directly if config doesn't exist yet
			members, err := groupManager.GetGroupMembers("group-1")
			if err == nil && len(members) > 0 {
				log.Info("Deploying to %d runners directly (no config yet)", len(members))
				for _, member := range members {
					// Use member.Name (runner-1, runner-2) instead of RunnerID (UUID)
					// because WebSocket clients are registered with their names
					runnerWSID := member.Name
					if runnerWSID == "" {
						runnerWSID = member.RunnerID // Fallback to UUID if name not set
					}
					
					deployMsg := websocket.Message{
						Type:      "DeployFlow",
						Timestamp: time.Now(),
						From:      "hub",
						To:        runnerWSID,
					}
					
					payload := map[string]interface{}{
						"flowId":     flow.ID,
						"version":    flow.Version,
						"accountId":  flow.AccountID,
						"definition": flow.Definition,
					}
					if req.Schedule != "" {
						payload["schedule"] = req.Schedule
					}
					
					payloadJSON, _ := json.Marshal(payload)
					deployMsg.Payload = payloadJSON
					wsHub.SendToRunner(runnerWSID, deployMsg)
					log.Info("Sent DeployFlow message to runner %s (name: %s)", runnerWSID, member.Name)
				}
			}
		} else if config != nil {
			log.Info("Deploying to %d runners from config", len(config.Members))
			for _, member := range config.Members {
				// Use member.Name (runner-1, runner-2) instead of RunnerID (UUID) 
				// because WebSocket clients are registered with their names
				runnerWSID := member.Name
				if runnerWSID == "" {
					runnerWSID = member.RunnerID // Fallback to UUID if name not set
				}
				
				deployMsg := websocket.Message{
					Type:      "DeployFlow",
					Timestamp: time.Now(),
					From:      "hub",
					To:        runnerWSID,
				}
				
				payload := map[string]interface{}{
					"flowId":     flow.ID,
					"version":    flow.Version,
					"accountId":  flow.AccountID,
					"definition": flow.Definition,
				}
				if req.Schedule != "" {
					payload["schedule"] = req.Schedule
				}
				
				payloadJSON, _ := json.Marshal(payload)
				deployMsg.Payload = payloadJSON
				wsHub.SendToRunner(runnerWSID, deployMsg)
				log.Info("Sent DeployFlow message to runner %s (name: %s, status: %s)", runnerWSID, member.Name, member.Status)
			}
		} else {
			log.Warn("Group config is nil for group-1")
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(flow)
	})
	
	mux.HandleFunc("/api/integrations/trigger", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		
		var req struct {
			AccountID string          `json:"accountId"`
			FlowID    string          `json:"flowId"`
			Input     json.RawMessage `json:"input,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		
		taskID, err := integrationManager.TriggerExecution(req.AccountID, req.FlowID, req.Input)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		
		// Send ExecuteTask message to all runners in the default group
		config, _ := groupManager.GetGroupConfig("group-1")
		if config != nil {
			execMsg := websocket.Message{
				Type:      "ExecuteTask",
				Timestamp: time.Now(),
				From:      "hub",
			}
			
			payload := map[string]interface{}{
				"taskId":    taskID,
				"flowId":    req.FlowID,
				"accountId": req.AccountID,
				"input":     req.Input,
			}
			if len(req.Input) == 0 {
				payload["input"] = json.RawMessage(`{"trigger":"hub"}`)
			}
			
			payloadJSON, _ := json.Marshal(payload)
			execMsg.Payload = payloadJSON
			
			// Broadcast to all runners
			wsHub.Broadcast(execMsg)
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"taskId": taskID})
	})
	
	// Connector API endpoints
	mux.HandleFunc("/api/connectors/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		
		// Handle "latest" version requests: /api/connectors/{name}/latest
		if strings.HasSuffix(path, "/latest") {
			parts := strings.Split(strings.TrimPrefix(strings.TrimSuffix(path, "/latest"), "/api/connectors/"), "/")
			if len(parts) != 1 {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid path"})
				return
			}
			
			name := parts[0]
			conn, err := connectorManager.GetLatestConnector(name)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			
			// Build download URL
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			downloadURL := fmt.Sprintf("%s://%s/api/connectors/%s/%s/download", scheme, r.Host, name, conn.Version)
			
			info := map[string]interface{}{
				"name":         conn.Name,
				"version":      conn.Version,
				"type":         conn.Type,
				"checksum":     conn.Checksum,
				"download_url": downloadURL,
			}
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(info)
			return
		}
		
		// Handle download requests
		if strings.HasSuffix(path, "/download") {
			parts := strings.Split(strings.TrimPrefix(strings.TrimSuffix(path, "/download"), "/api/connectors/"), "/")
			if len(parts) != 2 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			
			name := parts[0]
			version := parts[1]
			
			// Handle "latest" version by finding the latest version
			if version == "latest" {
				conn, err := connectorManager.GetLatestConnector(name)
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
					return
				}
				version = conn.Version
			}
			
			conn, err := connectorManager.GetConnector(name, version)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			
			// Serve binary
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-%s", name, version))
			w.Header().Set("X-Checksum", conn.Checksum)
			
			if err := connectorManager.ServeConnectorBinary(w, name, version); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			return
		}
		
		// Handle info requests
		parts := strings.Split(strings.TrimPrefix(path, "/api/connectors/"), "/")
		if len(parts) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid path"})
			return
		}
		
		name := parts[0]
		version := parts[1]
		
		// Handle "latest" version by finding the latest version
		if version == "latest" {
			conn, err := connectorManager.GetLatestConnector(name)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			version = conn.Version
		}
		
		conn, err := connectorManager.GetConnector(name, version)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		
		// Build download URL
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		downloadURL := fmt.Sprintf("%s://%s/api/connectors/%s/%s/download", scheme, r.Host, name, version)
		
		info := map[string]interface{}{
			"name":         conn.Name,
			"version":      conn.Version,
			"type":         conn.Type,
			"checksum":     conn.Checksum,
			"download_url": downloadURL,
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	})
	
	// Docker logs API endpoints
	if dockerClient != nil {
		mux.HandleFunc("/api/docker/containers", func(w http.ResponseWriter, r *http.Request) {
			containers, err := dockerClient.ListContainers(r.Context())
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(containers)
		})
		
		mux.HandleFunc("/api/docker/logs/", func(w http.ResponseWriter, r *http.Request) {
			containerName := strings.TrimPrefix(r.URL.Path, "/api/docker/logs/")
			if containerName == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "container name required"})
				return
			}
			
			// Parse since parameter (optional)
			since := time.Now().Add(-1 * time.Hour) // Default to last hour
			if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
				if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
					since = t
				}
			}
			
			follow := r.URL.Query().Get("follow") == "true"
			
			// Stream logs
			logs, err := dockerClient.StreamLogs(r.Context(), containerName, since, follow)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			
			// Set headers for streaming
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			
			// Write logs (or empty message if no logs)
			if logs == "" {
				w.Write([]byte("No logs available for this container.\n"))
			} else {
				w.Write([]byte(logs))
			}
		})
	}
	
	// Serve UI
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/index.html")
	})
	mux.Handle("/", http.FileServer(http.Dir("ui")))

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      mux,
		ReadTimeout:  cfg.ServerReadTimeout,
		WriteTimeout: cfg.ServerWriteTimeout,
	}

	// Start server in a goroutine
	go func() {
		log.Info("Runner Service listening on port %s", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down Runner Service...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("Server forced to shutdown: %v", err)
	}

	log.Info("Runner Service exited")
}

