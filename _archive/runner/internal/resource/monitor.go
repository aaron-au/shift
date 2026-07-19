package resource

import (
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shift/runner/internal/logger"
)

// Metrics represents current resource usage metrics
type Metrics struct {
	CPUPercent    float64   `json:"cpu_percent"`
	MemoryPercent float64   `json:"memory_percent"`
	MemoryUsedMB  uint64    `json:"memory_used_mb"`
	MemoryTotalMB uint64    `json:"memory_total_mb"`
	ActiveTasks   int       `json:"active_tasks"`
	Timestamp     time.Time `json:"timestamp"`
}

// Monitor tracks resource usage
type Monitor struct {
	logger      *logger.Logger
	current     Metrics
	mu          sync.RWMutex
	activeTasks int
	stopChan    chan struct{}
}

// NewMonitor creates a new resource monitor
func NewMonitor(log *logger.Logger) *Monitor {
	return &Monitor{
		logger:   log,
		stopChan: make(chan struct{}),
	}
}

// Start starts monitoring resources
func (m *Monitor) Start() {
	go m.monitorLoop()
	m.logger.Info("Resource monitor started")
}

// Stop stops monitoring resources
func (m *Monitor) Stop() {
	close(m.stopChan)
	m.logger.Info("Resource monitor stopped")
}

// GetMetrics returns current resource metrics
func (m *Monitor) GetMetrics() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// SetActiveTasks updates the active task count
func (m *Monitor) SetActiveTasks(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTasks = count
	m.current.ActiveTasks = count
}

// IncrementActiveTasks increments active task count
func (m *Monitor) IncrementActiveTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTasks++
	m.current.ActiveTasks = m.activeTasks
}

// DecrementActiveTasks decrements active task count
func (m *Monitor) DecrementActiveTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeTasks > 0 {
		m.activeTasks--
		m.current.ActiveTasks = m.activeTasks
	}
}

// GetLoadScore returns a load score (0-100, lower is better)
func (m *Monitor) GetLoadScore() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Weighted score: 50% CPU, 30% Memory, 20% Active Tasks
	cpuWeight := m.current.CPUPercent * 0.5
	memWeight := m.current.MemoryPercent * 0.3
	taskWeight := float64(m.current.ActiveTasks) * 10.0 // Each task adds 10 points
	if taskWeight > 20 {
		taskWeight = 20
	}
	
	score := cpuWeight + memWeight + taskWeight
	if score > 100 {
		score = 100
	}
	
	return score
}

func (m *Monitor) monitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.updateMetrics()
		}
	}
}

func (m *Monitor) updateMetrics() {
	// Get CPU usage
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		m.logger.Error("Failed to get CPU usage: %v", err)
		return
	}

	// Get memory usage
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		m.logger.Error("Failed to get memory usage: %v", err)
		return
	}

	m.mu.Lock()
	m.current.CPUPercent = cpuPercent[0]
	m.current.MemoryPercent = memInfo.UsedPercent
	m.current.MemoryUsedMB = memInfo.Used / 1024 / 1024
	m.current.MemoryTotalMB = memInfo.Total / 1024 / 1024
	m.current.ActiveTasks = m.activeTasks
	m.current.Timestamp = time.Now()
	m.mu.Unlock()
}

// GetRuntimeStats returns basic runtime statistics
func GetRuntimeStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	return map[string]interface{}{
		"goroutines": runtime.NumGoroutine(),
		"memory_alloc_mb": bToMb(m.Alloc),
		"memory_total_alloc_mb": bToMb(m.TotalAlloc),
		"memory_sys_mb": bToMb(m.Sys),
		"num_gc": m.NumGC,
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

