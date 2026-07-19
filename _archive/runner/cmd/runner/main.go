package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shift/runner/internal/config"
	"github.com/shift/runner/internal/connector"
	"github.com/shift/runner/internal/execution"
	"github.com/shift/runner/internal/kafka"
	"github.com/shift/runner/internal/logger"
	"github.com/shift/runner/internal/resource"
	"github.com/shift/runner/internal/runnergroup"
	"github.com/shift/runner/internal/scheduler"
	"github.com/shift/runner/internal/taskqueue"
	"github.com/shift/runner/internal/timesync"
	"github.com/shift/runner/internal/websocket"
)

func main() {
	// Initialize logger
	log := logger.New()
	log.Info("Starting Runner...")

	// Load configuration
	cfg := config.Load()

	// Generate runner ID if not set
	runnerID := cfg.RunnerID
	if runnerID == "" {
		runnerID = uuid.New().String()
	}

	log.Info("Runner ID: %s", runnerID)
	log.Info("Runner Name: %s", cfg.RunnerName)
	log.Info("Runner Group ID: %s", cfg.RunnerGroupID)

	// Initialize task queue (SQLite)
	dbPath := filepath.Join(os.TempDir(), "shift-runner-"+runnerID+".db")
	taskQueue, err := taskqueue.NewQueue(dbPath, log)
	if err != nil {
		log.Fatal("Failed to initialize task queue: %v", err)
	}
	defer taskQueue.Close()

	// Initialize connector loader (for dynamic connector loading from Hub)
	connectorLoader := connector.NewLoader(log)
	
	// Initialize connector registry
	connectorRegistry := connector.NewRegistry()
	
	// Note: Connectors are now loaded dynamically from Hub when flows are deployed
	// Built-in connectors are still available for backward compatibility
	// but will be replaced by Hub-provided connectors
	
	// Initialize execution engine with connector registry
	execEngine := execution.NewEngine(log, connectorRegistry)

	// Initialize time sync manager (must be before scheduler)
	timeSync := timesync.NewTimeSync(log)

	// Initialize resource monitor
	resourceMonitor := resource.NewMonitor(log)
	resourceMonitor.Start()
	defer resourceMonitor.Stop()

	// Initialize scheduler with resource monitor
	taskScheduler := scheduler.NewScheduler(taskQueue, resourceMonitor, log)
	// Set synchronized time function
	taskScheduler.SetTimeNow(func() time.Time {
		return timeSync.Now()
	})
	taskScheduler.SetTaskCreateCallback(func(task *taskqueue.Task) {
		log.Info("Scheduled task created: %s", task.ID)
	})
	taskScheduler.Start()
	defer taskScheduler.Stop()

	// Initialize Kafka consumer
	var kafkaConsumer *kafka.Consumer
	if cfg.KafkaBrokers != "" {
		brokers := strings.Split(cfg.KafkaBrokers, ",")
		consumer, err := kafka.NewConsumer(brokers, "shift-runner-group-"+cfg.RunnerGroupID, log)
		if err != nil {
			log.Error("Failed to create Kafka consumer: %v", err)
		} else {
			kafkaConsumer = consumer
			kafkaConsumer.SetMessageHandler(func(topic string, key string, value []byte) {
				log.Info("Received Kafka message from topic %s: %s", topic, key)
				
				// Parse Kafka message to extract flow trigger info
				var kafkaMsg map[string]interface{}
				if err := json.Unmarshal(value, &kafkaMsg); err != nil {
					log.Error("Failed to unmarshal Kafka message: %v", err)
					return
				}
				
				flowID, ok := kafkaMsg["flow_id"].(string)
				if !ok {
					log.Error("Kafka message missing flow_id")
					return
				}
				
				accountID, _ := kafkaMsg["account_id"].(string)
				if accountID == "" {
					accountID = "default-account"
				}
				
				// Create task from Kafka message
				taskID := uuid.New().String()
				inputPayload, _ := json.Marshal(map[string]interface{}{
					"trigger": "kafka",
					"topic":   topic,
					"key":     key,
					"message": kafkaMsg,
				})
				
				task := &taskqueue.Task{
					ID:          taskID,
					FlowID:      flowID,
					AccountID:   accountID,
					Status:      "pending",
					InputPayload: inputPayload,
					CreatedAt:   time.Now(),
				}
				
				if err := taskQueue.AddTask(task); err != nil {
					log.Error("Failed to add Kafka-triggered task: %v", err)
				} else {
					log.Info("Added Kafka-triggered task %s for flow %s", taskID, flowID)
				}
			})
			
			// Start Kafka consumer in background
			go func() {
				ctx := context.Background()
				if err := kafkaConsumer.Subscribe(ctx, []string{"shift-integrations"}); err != nil {
					log.Error("Kafka consumer error: %v", err)
				}
			}()
			defer kafkaConsumer.Close()
		}
	}

	// Initialize runner group manager
	groupManager := runnergroup.NewManager(log)

	// Initialize P2P WebSocket hub first
	var hubClient *websocket.HubClient
		p2pHub := websocket.NewP2PHub(log, func(msg websocket.Message) {
		// Handle P2P messages
		switch msg.Type {
		case "ResourceMetrics":
			// Update scheduler with peer metrics for coordination
			var metrics resource.Metrics
			if err := json.Unmarshal(msg.Payload, &metrics); err == nil {
				taskScheduler.UpdatePeerMetrics(msg.From, metrics)
				log.Debug("Updated metrics for peer %s: CPU=%.2f%%, Memory=%.2f%%, Tasks=%d", 
					msg.From, metrics.CPUPercent, metrics.MemoryPercent, metrics.ActiveTasks)
			}
		case "TimeSync":
			// Sync time with peer
			var timeSyncMsg struct {
				ServerTime int64 `json:"server_time"`
				Offset     int64 `json:"offset"` // nanoseconds
			}
			if err := json.Unmarshal(msg.Payload, &timeSyncMsg); err == nil {
				timeSync.SyncWithPeer(timeSyncMsg.ServerTime, time.Duration(timeSyncMsg.Offset))
			}
		}
	})
	go p2pHub.Run()

	// Initialize Hub WebSocket client with message handlers
	hubClient = websocket.NewHubClient(cfg.HubWSURL, runnerID, log, func(msg websocket.Message) {
		log.Info("Received message from Hub: %s", msg.Type)
		
		switch msg.Type {
		case "GroupConfig":
			if err := groupManager.UpdateConfig(msg.Payload); err != nil {
				log.Error("Failed to update group config: %v", err)
			} else {
				peers := groupManager.GetPeerEndpoints(runnerID)
				log.Info("Group config updated, connecting to %d peers", len(peers))
			}
		
		case "TimeSync":
			var timeSyncMsg struct {
				ServerTime int64  `json:"server_time"`
				Timezone   string `json:"timezone"`
			}
			if err := json.Unmarshal(msg.Payload, &timeSyncMsg); err != nil {
				log.Error("Failed to unmarshal TimeSync message: %v", err)
			} else {
				timeSync.SyncWithHub(timeSyncMsg.ServerTime, timeSyncMsg.Timezone)
			}
		
		case "DeployFlow":
			var deployMsg struct {
				FlowID     string          `json:"flowId"`
				Version    int             `json:"version"`
				Definition json.RawMessage `json:"definition"`
				AccountID  string          `json:"accountId"`
				Schedule   string          `json:"schedule,omitempty"` // Cron expression
			}
			if err := json.Unmarshal(msg.Payload, &deployMsg); err != nil {
				log.Error("Failed to unmarshal DeployFlow message: %v", err)
			} else {
				// Extract connectors needed from flow definition
				var flowDef map[string]interface{}
				if err := json.Unmarshal(deployMsg.Definition, &flowDef); err == nil {
					if steps, ok := flowDef["steps"].([]interface{}); ok {
						connectorsNeeded := make(map[string]string) // name -> version
						
						for _, step := range steps {
							if stepMap, ok := step.(map[string]interface{}); ok {
								if connectorName, ok := stepMap["connector"].(string); ok {
									// Default to latest version if not specified
									version := "latest"
									if v, ok := stepMap["connector_version"].(string); ok {
										version = v
									}
									connectorsNeeded[connectorName] = version
								}
							}
						}
						
						// Request connectors from Hub
						for name, version := range connectorsNeeded {
							log.Info("Requesting connector %s version %s from Hub", name, version)
							
							// Request connector info and download from Hub
							loadedConn, err := connectorLoader.RequestConnector(cfg.HubAPIURL, name, version)
							if err != nil {
								log.Error("Failed to load connector %s version %s from Hub: %v", name, version, err)
								// Do NOT fallback to built-in - connectors must come from Hub
								// This ensures version control and proper connector management
								continue
							}
							
							// Register the connector from the plugin
							if loadedConn.Connector == nil {
								log.Error("Connector %s version %s loaded but Connector is nil", name, loadedConn.Version)
								continue
							}
							
							connectorRegistry.Register(name, loadedConn.Connector)
							log.Info("Registered connector %s version %s from Hub plugin (checksum: %s, path: %s)", 
								name, loadedConn.Version, loadedConn.Checksum, loadedConn.Path)
						}
					}
				}
				
				// Deploy the flow
				if err := execEngine.DeployFlow(deployMsg.Definition); err != nil {
					log.Error("Failed to deploy flow: %v", err)
				} else {
					log.Info("Deployed flow %s version %d", deployMsg.FlowID, deployMsg.Version)
					
					// If schedule is provided, schedule the flow
					if deployMsg.Schedule != "" {
						scheduledFlow := &scheduler.ScheduledFlow{
							FlowID:    deployMsg.FlowID,
							AccountID: deployMsg.AccountID,
							Schedule:  deployMsg.Schedule,
						}
						if err := taskScheduler.ScheduleFlow(scheduledFlow); err != nil {
							log.Error("Failed to schedule flow: %v", err)
						} else {
							log.Info("Scheduled flow %s with schedule %s", deployMsg.FlowID, deployMsg.Schedule)
						}
					}
				}
			}
		
		case "ExecuteTask":
			var execMsg struct {
				TaskID     string          `json:"taskId"`
				FlowID     string          `json:"flowId"`
				AccountID  string          `json:"accountId"`
				Input      json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(msg.Payload, &execMsg); err != nil {
				log.Error("Failed to unmarshal ExecuteTask message: %v", err)
			} else {
				task := &taskqueue.Task{
					ID:          execMsg.TaskID,
					FlowID:      execMsg.FlowID,
					AccountID:   execMsg.AccountID,
					Status:      "pending",
					InputPayload: execMsg.Input,
					CreatedAt:   time.Now(),
				}
				if err := taskQueue.AddTask(task); err != nil {
					log.Error("Failed to add task: %v", err)
				} else {
					log.Info("Added task %s for flow %s", execMsg.TaskID, execMsg.FlowID)
				}
			}
		}
		
		// Forward Hub messages to P2P peers
		p2pHub.Broadcast(msg)
	})

	// Send periodic resource metrics to P2P peers (not Hub)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				metrics := resourceMonitor.GetMetrics()
				metricsJSON, _ := json.Marshal(metrics)
				
				metricsMsg := websocket.Message{
					Type:      "ResourceMetrics",
					Timestamp: time.Now(),
					From:      runnerID,
					Payload:   metricsJSON,
				}
				// Broadcast to P2P peers
				p2pHub.Broadcast(metricsMsg)
				
				// Update scheduler with our own metrics
				taskScheduler.UpdatePeerMetrics(runnerID, metrics)
			}
		}
	}()

	// Set up execution completion callback to report to Hub
	execEngine.SetExecutionCompleteCallback(func(result *execution.ExecutionResult) {
		statusMsg := websocket.Message{
			Type:      "ExecutionStatus",
			Timestamp: time.Now(),
			From:      runnerID,
		}
		
		// Get task details for reporting
		task, err := taskQueue.GetTask(result.TaskID)
		if err != nil {
			log.Error("Failed to get task for status report: %v", err)
			return
		}
		
		statusPayload := map[string]interface{}{
			"task_id":        result.TaskID,
			"account_id":     task.AccountID,
			"flow_id":        task.FlowID,
			"status":         result.Status,
			"duration_ms":    result.DurationMs,
			"cpu_time_ms":    result.CPUTimeMs,
			"memory_used_mb": result.MemoryUsedMB,
			"connectors_used": result.ConnectorsUsed,
			"started_at":     result.StartedAt.Format(time.RFC3339),
			"completed_at":   result.CompletedAt.Format(time.RFC3339),
		}
		
		if result.Error != "" {
			statusPayload["error"] = result.Error
		} else {
			statusPayload["output"] = json.RawMessage(result.Output)
		}
		
		statusJSON, _ := json.Marshal(statusPayload)
		statusMsg.Payload = statusJSON
		
		if hubClient != nil {
			hubClient.Send(statusMsg)
			log.Info("Sent execution status for task %s to Hub (CPU: %dms, Memory: %dMB, Connectors: %v)", 
				result.TaskID, result.CPUTimeMs, result.MemoryUsedMB, result.ConnectorsUsed)
		}
	})

	// Set up group config update callback
	groupManager.SetConfigUpdateCallback(func(config *runnergroup.GroupConfig) {
		log.Info("Group config updated: %d members", len(config.Members))
		// TODO: Establish connections to peers
	})

	// Setup HTTP server for P2P and UI
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// P2P WebSocket endpoint
	mux.HandleFunc("/p2p", func(w http.ResponseWriter, r *http.Request) {
		peerID := r.URL.Query().Get("peer_id")
		if peerID == "" {
			peerID = runnerID
		}
		p2pHub.HandleP2PWebSocket(w, r, peerID)
	})

	// API endpoints
	mux.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		metrics := resourceMonitor.GetMetrics()
		json.NewEncoder(w).Encode(metrics)
	})

	mux.HandleFunc("/api/scheduled-tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		tasks := taskScheduler.GetScheduledTasks()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"scheduled_tasks": tasks,
			"resource_metrics": resourceMonitor.GetMetrics(),
		})
	})
	
	mux.HandleFunc("/api/connectors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		// List all loaded connectors from registry and loader
		connectors := make([]map[string]interface{}, 0)
		
		// Get connectors from registry (both built-in and dynamically loaded)
		connectorNames := []string{"http", "sleep"}
		for _, name := range connectorNames {
			if conn, exists := connectorRegistry.Get(name); exists {
				connectors = append(connectors, map[string]interface{}{
					"name":      name,
					"type":      "registered",
					"available": true,
					"loaded":    conn != nil,
				})
			}
		}
		
		// Also check loader for dynamically loaded connectors
		loadedConnectors := connectorLoader.ListConnectors()
		for _, loadedConn := range loadedConnectors {
			// Check if already in list
			found := false
			for _, c := range connectors {
				if c["name"] == loadedConn.Name {
					found = true
					c["version"] = loadedConn.Version
					c["checksum"] = loadedConn.Checksum
					c["path"] = loadedConn.Path
					break
				}
			}
			if !found {
				connectors = append(connectors, map[string]interface{}{
					"name":      loadedConn.Name,
					"version":   loadedConn.Version,
					"checksum":  loadedConn.Checksum,
					"path":      loadedConn.Path,
					"type":      "dynamic",
					"available": true,
					"loaded":    true,
				})
			}
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connectors": connectors,
			"total":      len(connectors),
		})
	})

	// UI endpoint
	mux.Handle("/", http.FileServer(http.Dir("ui")))

	server := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      mux,
		ReadTimeout:  cfg.ServerReadTimeout,
		WriteTimeout: cfg.ServerWriteTimeout,
	}

	// Start server in a goroutine
	go func() {
		log.Info("Runner listening on port %s", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed to start: %v", err)
		}
	}()


	// Connect to Hub with retry (include group_id in connection URL)
	go func() {
		for {
			hubURL := cfg.HubWSURL + "?runner_id=" + runnerID + "&group_id=" + cfg.RunnerGroupID + "&p2p_port=" + cfg.P2PPort
			if err := hubClient.Connect(hubURL); err != nil {
				log.Error("Failed to connect to Hub: %v, retrying in 5 seconds...", err)
				time.Sleep(5 * time.Second)
				continue
			}
			break
		}
	}()

	// Start task worker to process tasks from queue
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Try to claim a task
				task, err := taskQueue.ClaimTask(runnerID)
				if err != nil {
					log.Error("Failed to claim task: %v", err)
					// If we have a persistent error claiming tasks, try to find and report failed tasks
					// This handles cases where tasks are stuck due to database errors
					go func() {
						// Try to get any pending tasks that might be stuck
						pendingTasks, _ := taskQueue.GetPendingTasks()
						for _, stuckTask := range pendingTasks {
							// If a task has been pending for more than 5 minutes, mark it as failed
							if time.Since(stuckTask.CreatedAt) > 5*time.Minute {
								log.Warn("Marking stuck task %s as failed (pending for %v)", stuckTask.ID, time.Since(stuckTask.CreatedAt))
								taskQueue.UpdateTaskStatus(stuckTask.ID, "failed", nil, fmt.Sprintf("Task claim failed: %v", err))
								
								// Report to Hub
								if hubClient != nil {
									statusMsg := websocket.Message{
										Type:      "ExecutionStatus",
										Timestamp: time.Now(),
										From:      runnerID,
									}
									statusPayload := map[string]interface{}{
										"task_id":    stuckTask.ID,
										"account_id": stuckTask.AccountID,
										"flow_id":    stuckTask.FlowID,
										"status":     "failed",
										"error":      fmt.Sprintf("Task claim failed: %v", err),
									}
									statusJSON, _ := json.Marshal(statusPayload)
									statusMsg.Payload = statusJSON
									hubClient.Send(statusMsg)
								}
							}
						}
					}()
					continue
				}
				if task == nil {
					continue // No pending tasks
				}
				
				// Update status to running
				taskQueue.UpdateTaskStatus(task.ID, "running", nil, "")
				resourceMonitor.IncrementActiveTasks()
				
				// Execute the task
				go func(t *taskqueue.Task) {
					defer resourceMonitor.DecrementActiveTasks()
					
					result, err := execEngine.ExecuteTask(ctx, t)
					if err != nil {
						log.Error("Task execution failed: %v", err)
						taskQueue.UpdateTaskStatus(t.ID, "failed", nil, err.Error())
					} else {
						taskQueue.UpdateTaskStatus(t.ID, result.Status, result.Output, result.Error)
					}
				}(task)
			}
		}
	}()

	// Send periodic heartbeat to Hub
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				heartbeat := websocket.Message{
					Type:      "Heartbeat",
					Payload:   []byte(`{"status":"online"}`),
					Timestamp: time.Now(),
					From:      runnerID,
				}
				hubClient.Send(heartbeat)
			}
		}
	}()

	// Broadcast resource metrics and time sync to peers periodically
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Broadcast resource metrics
				metrics := resourceMonitor.GetMetrics()
				metricsJSON, _ := json.Marshal(metrics)
				p2pHub.Broadcast(websocket.Message{
					Type:      "ResourceMetrics",
					Payload:   metricsJSON,
					Timestamp: timeSync.Now(),
					From:      runnerID,
				})
				
				// Broadcast time sync information
				timeSyncJSON, _ := json.Marshal(map[string]interface{}{
					"server_time": timeSync.Now().Unix(),
					"offset":      timeSync.GetOffset().Nanoseconds(),
				})
				p2pHub.Broadcast(websocket.Message{
					Type:      "TimeSync",
					Payload:   timeSyncJSON,
					Timestamp: timeSync.Now(),
					From:      runnerID,
				})
			}
		}
	}()

	// Forward P2P messages to Hub (this will be handled by the P2P hub internally)
	// We'll send messages directly when they're received in the P2P hub

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down Runner...")

	hubClient.Close()
	cancel() // Stop task worker

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("Server forced to shutdown: %v", err)
	}

	log.Info("Runner exited")
}

