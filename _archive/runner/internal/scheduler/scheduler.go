package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/shift/runner/internal/logger"
	"github.com/shift/runner/internal/resource"
	"github.com/shift/runner/internal/taskqueue"
)

// ScheduledFlow represents a flow that should be executed on a schedule
type ScheduledFlow struct {
	FlowID    string
	AccountID string
	Schedule  string // Cron expression
	FlowName  string
}

// Scheduler manages scheduled flow executions with coordination
type Scheduler struct {
	cron          *cron.Cron
	taskQueue     *taskqueue.Queue
	logger        *logger.Logger
	resourceMonitor *resource.Monitor
	schedules     map[string]cron.EntryID // flowID -> entryID
	schedulesMu   sync.RWMutex
	onTaskCreate  func(*taskqueue.Task) // Callback when a scheduled task is created
	peerMetrics   map[string]resource.Metrics // runnerID -> metrics
	peerMetricsMu sync.RWMutex
	timeNow       func() time.Time // Function to get current synchronized time
}

// NewScheduler creates a new scheduler
func NewScheduler(taskQueue *taskqueue.Queue, resourceMonitor *resource.Monitor, log *logger.Logger) *Scheduler {
	c := cron.New(cron.WithSeconds()) // Support seconds-level precision
	
	return &Scheduler{
		cron:           c,
		taskQueue:      taskQueue,
		logger:         log,
		resourceMonitor: resourceMonitor,
		schedules:      make(map[string]cron.EntryID),
		peerMetrics:    make(map[string]resource.Metrics),
		timeNow:        time.Now, // Default to system time, can be overridden
	}
}

// SetTimeNow sets the function to get current synchronized time
func (s *Scheduler) SetTimeNow(fn func() time.Time) {
	s.timeNow = fn
}

// UpdatePeerMetrics updates resource metrics for a peer runner
func (s *Scheduler) UpdatePeerMetrics(runnerID string, metrics resource.Metrics) {
	s.peerMetricsMu.Lock()
	defer s.peerMetricsMu.Unlock()
	s.peerMetrics[runnerID] = metrics
}

// GetBestRunner returns the runner ID with the lowest load score
func (s *Scheduler) GetBestRunner(selfID string) string {
	s.peerMetricsMu.RLock()
	defer s.peerMetricsMu.RUnlock()
	
	bestRunner := selfID
	bestScore := s.resourceMonitor.GetLoadScore()
	
	// Check self
	if s.resourceMonitor != nil {
		bestScore = s.resourceMonitor.GetLoadScore()
	}
	
	// Check peers
	for runnerID, metrics := range s.peerMetrics {
		// Calculate load score for peer (simplified - would need full monitor)
		score := metrics.CPUPercent*0.5 + metrics.MemoryPercent*0.3 + float64(metrics.ActiveTasks)*10.0
		if score > 100 {
			score = 100
		}
		
		if score < bestScore {
			bestScore = score
			bestRunner = runnerID
		}
	}
	
	return bestRunner
}

// SetTaskCreateCallback sets a callback for when scheduled tasks are created
func (s *Scheduler) SetTaskCreateCallback(callback func(*taskqueue.Task)) {
	s.onTaskCreate = callback
}

// Start starts the scheduler
func (s *Scheduler) Start() {
	s.cron.Start()
	s.logger.Info("Scheduler started")
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.cron.Stop()
	s.logger.Info("Scheduler stopped")
}

// ScheduleFlow schedules a flow to run on a cron schedule
func (s *Scheduler) ScheduleFlow(flow *ScheduledFlow) error {
	// Remove existing schedule if any
	s.UnscheduleFlow(flow.FlowID)

	entryID, err := s.cron.AddFunc(flow.Schedule, func() {
		s.executeScheduledFlow(flow)
	})
	if err != nil {
		return fmt.Errorf("invalid cron expression '%s': %w", flow.Schedule, err)
	}

	s.schedulesMu.Lock()
	s.schedules[flow.FlowID] = entryID
	s.schedulesMu.Unlock()

	s.logger.Info("Scheduled flow %s with schedule %s", flow.FlowID, flow.Schedule)
	return nil
}

// UnscheduleFlow removes a scheduled flow
func (s *Scheduler) UnscheduleFlow(flowID string) {
	s.schedulesMu.Lock()
	defer s.schedulesMu.Unlock()

	if entryID, exists := s.schedules[flowID]; exists {
		s.cron.Remove(entryID)
		delete(s.schedules, flowID)
		s.logger.Info("Unscheduled flow %s", flowID)
	}
}

// executeScheduledFlow executes a scheduled flow with resource-aware coordination
// Only the runner with lowest resource usage creates the task
func (s *Scheduler) executeScheduledFlow(flow *ScheduledFlow) {
	// Create a deterministic task ID based on flow ID and schedule time
	// This ensures all runners know which task to look for
	now := s.timeNow() // Use synchronized time
	scheduleKey := fmt.Sprintf("%s:%s:%d", flow.FlowID, flow.Schedule, now.Unix())
	taskIDHash := sha256.Sum256([]byte(scheduleKey))
	taskID := hex.EncodeToString(taskIDHash[:16]) // Use first 16 bytes as UUID-like ID

	// Check if this runner should create the task based on resource usage
	// Add a small random delay based on load score to give lower-load runners priority
	loadScore := s.resourceMonitor.GetLoadScore()
	
	// Delay inversely proportional to load (lower load = shorter delay)
	// This gives runners with lower resource usage a head start
	delayMs := int(loadScore * 10) // 0-1000ms delay
	if delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}

	// Try to add the task - this will fail if another runner already added it
	// The runner with lowest resources (shortest delay) is most likely to succeed first
	task := &taskqueue.Task{
		ID:          taskID,
		FlowID:      flow.FlowID,
		AccountID:   flow.AccountID,
		Status:      "pending",
		InputPayload: json.RawMessage(`{"trigger":"schedule","scheduled_at":"` + now.Format(time.RFC3339) + `"}`),
		CreatedAt:   now,
	}

	err := s.taskQueue.AddTask(task)
	if err != nil {
		// Task already exists (another runner with lower resources created it) - this is expected
		s.logger.Debug("Scheduled task %s already exists (created by runner with lower resources, load_score=%.2f)", taskID, loadScore)
		return
	}

	s.logger.Info("Created scheduled task %s for flow %s (load_score=%.2f)", taskID, flow.FlowID, loadScore)

	// Notify callback if set
	if s.onTaskCreate != nil {
		s.onTaskCreate(task)
	}
}

// GetScheduledTasks returns information about scheduled tasks
func (s *Scheduler) GetScheduledTasks() []map[string]interface{} {
	s.schedulesMu.RLock()
	defer s.schedulesMu.RUnlock()
	
	tasks := make([]map[string]interface{}, 0, len(s.schedules))
	for flowID := range s.schedules {
		tasks = append(tasks, map[string]interface{}{
			"flow_id": flowID,
		})
	}
	return tasks
}

// ListScheduledFlows returns all scheduled flow IDs
func (s *Scheduler) ListScheduledFlows() []string {
	s.schedulesMu.RLock()
	defer s.schedulesMu.RUnlock()

	flowIDs := make([]string, 0, len(s.schedules))
	for flowID := range s.schedules {
		flowIDs = append(flowIDs, flowID)
	}
	return flowIDs
}

