package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shift/runner/internal/connector"
	"github.com/shift/runner/internal/logger"
	"github.com/shift/runner/internal/taskqueue"
)

// getCurrentMemoryUsage returns current memory usage in bytes
func getCurrentMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// extractConnectorsFromFlow extracts connector names from flow definition
func extractConnectorsFromFlow(flow *FlowDefinition) []string {
	connectors := make(map[string]bool)
	
	for _, step := range flow.Steps {
		if step.Connector != "" {
			connectors[step.Connector] = true
		}
	}
	
	result := make([]string, 0, len(connectors))
	for connector := range connectors {
		result = append(result, connector)
	}
	
	return result
}

// FlowDefinition represents an integration flow definition
type FlowDefinition struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Version     int                    `json:"version"`
	Steps       []FlowStep             `json:"steps"`
	AccountID   string                 `json:"account_id"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// FlowStep represents a step in an integration flow
type FlowStep struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Connector   string                 `json:"connector"`
	Action      string                 `json:"action"`
	Config      map[string]interface{} `json:"config"`
	InputMapping map[string]interface{} `json:"input_mapping,omitempty"`
}

// ExecutionResult represents the result of an integration execution
type ExecutionResult struct {
	TaskID         string
	Status         string // completed, failed
	Output         json.RawMessage
	Error          string
	DurationMs     int64
	CPUTimeMs      int64    // CPU time used during execution
	MemoryUsedMB   uint64   // Peak memory used during execution
	ConnectorsUsed []string // List of connectors used
	StartedAt      time.Time
	CompletedAt    time.Time
}

// Engine executes integration flows
type Engine struct {
	flows              map[string]*FlowDefinition // flowID -> flow definition
	flowsLock          sync.RWMutex
	logger             *logger.Logger
	connectorRegistry  *connector.Registry
	onExecutionComplete func(*ExecutionResult)
}

// NewEngine creates a new execution engine
func NewEngine(log *logger.Logger, connectorRegistry *connector.Registry) *Engine {
	return &Engine{
		flows:             make(map[string]*FlowDefinition),
		logger:            log,
		connectorRegistry: connectorRegistry,
	}
}

// SetExecutionCompleteCallback sets a callback for when executions complete
func (e *Engine) SetExecutionCompleteCallback(callback func(*ExecutionResult)) {
	e.onExecutionComplete = callback
}

// DeployFlow deploys a flow definition to the engine
func (e *Engine) DeployFlow(flowJSON json.RawMessage) error {
	var flow FlowDefinition
	if err := json.Unmarshal(flowJSON, &flow); err != nil {
		return fmt.Errorf("failed to unmarshal flow: %w", err)
	}

	e.flowsLock.Lock()
	e.flows[flow.ID] = &flow
	e.flowsLock.Unlock()

	e.logger.Info("Deployed flow: %s (version %d)", flow.Name, flow.Version)
	return nil
}

// ExecuteTask executes a task using the appropriate flow
func (e *Engine) ExecuteTask(ctx context.Context, task *taskqueue.Task) (*ExecutionResult, error) {
	e.flowsLock.RLock()
	flow, exists := e.flows[task.FlowID]
	e.flowsLock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("flow %s not found", task.FlowID)
	}

	startedAt := time.Now()
	if task.StartedAt != nil {
		startedAt = *task.StartedAt
	}

	e.logger.Info("Executing task %s for flow %s", task.ID, flow.Name)

	// Simulate execution with phases: accepted -> running -> completed
	// In a real implementation, this would execute the flow steps using connectors
	
	// Phase 1: Accepted (already done when task was claimed)
	time.Sleep(100 * time.Millisecond) // Simulate processing time
	
	// Phase 2: Running
	e.logger.Info("Task %s is now running", task.ID)
	
	// Track resource usage during execution
	execStartCPU := time.Now()
	execStartMem := getCurrentMemoryUsage()
	
	// Phase 3: Execute flow steps using connectors
	output := e.executeFlowSteps(ctx, flow, task.InputPayload)
	
	completedAt := time.Now()
	duration := completedAt.Sub(startedAt).Milliseconds()
	
	// Calculate resource usage
	cpuTimeMs := completedAt.Sub(execStartCPU).Milliseconds()
	execEndMem := getCurrentMemoryUsage()
	memoryUsedMB := uint64(0)
	if execEndMem > execStartMem {
		memoryUsedMB = (execEndMem - execStartMem) / 1024 / 1024
	}
	
	// Extract connectors used from flow definition
	connectorsUsed := extractConnectorsFromFlow(flow)

	result := &ExecutionResult{
		TaskID:         task.ID,
		Status:         "completed",
		Output:         output,
		DurationMs:     duration,
		CPUTimeMs:      cpuTimeMs,
		MemoryUsedMB:   memoryUsedMB,
		ConnectorsUsed: connectorsUsed,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
	}

	e.logger.Info("Task %s completed in %dms", task.ID, duration)

	if e.onExecutionComplete != nil {
		e.onExecutionComplete(result)
	}

	return result, nil
}

// executeFlowSteps executes the steps of a flow using connectors
func (e *Engine) executeFlowSteps(ctx context.Context, flow *FlowDefinition, input json.RawMessage) json.RawMessage {
	// Parse input to determine trigger type
	var inputMap map[string]interface{}
	json.Unmarshal(input, &inputMap)
	
	trigger := "unknown"
	if t, ok := inputMap["trigger"].(string); ok {
		trigger = t
	}
	
	stepResults := make([]map[string]interface{}, 0)
	lastOutput := inputMap
	
	// Execute each step in sequence
	for i, step := range flow.Steps {
		e.logger.Info("Executing step %d/%d: %s (%s)", i+1, len(flow.Steps), step.ID, step.Action)
		
		// Get connector
		conn, exists := e.connectorRegistry.Get(step.Connector)
		if !exists {
			e.logger.Error("Connector %s not found", step.Connector)
			stepResults = append(stepResults, map[string]interface{}{
				"step_id": step.ID,
				"error":   fmt.Sprintf("connector %s not found", step.Connector),
			})
			continue
		}
		
		// Prepare step input (merge step config with input mapping)
		stepInput := make(map[string]interface{})
		
		// Copy step config
		for k, v := range step.Config {
			stepInput[k] = v
		}
		
		// Apply input mapping (if any)
		if step.InputMapping != nil {
			for k, v := range step.InputMapping {
				// Resolve values from last output
				if path, ok := v.(string); ok && strings.HasPrefix(path, "$.") {
					// Simple path resolution (could be enhanced)
					stepInput[k] = resolvePath(lastOutput, path)
				} else {
					stepInput[k] = v
				}
			}
		}
		
		// Execute step
		stepOutput, err := conn.Execute(ctx, step.Action, stepInput)
		if err != nil {
			e.logger.Error("Step %s failed: %v", step.ID, err)
			stepResults = append(stepResults, map[string]interface{}{
				"step_id": step.ID,
				"error":   err.Error(),
			})
			break
		}
		
		stepResults = append(stepResults, map[string]interface{}{
			"step_id": step.ID,
			"action":  step.Action,
			"output":  stepOutput,
		})
		
		lastOutput = stepOutput
	}
	
	// Build final result
	result := map[string]interface{}{
		"flow_id":       flow.ID,
		"flow_name":     flow.Name,
		"steps_executed": len(stepResults),
		"trigger":       trigger,
		"steps":         stepResults,
		"input":         json.RawMessage(input),
		"output":        lastOutput,
		"executed_at":   time.Now().Format(time.RFC3339),
	}

	outputJSON, _ := json.Marshal(result)
	return outputJSON
}

// resolvePath resolves a JSON path like "$.field" from a map
func resolvePath(data map[string]interface{}, path string) interface{} {
	if !strings.HasPrefix(path, "$.") {
		return path
	}
	
	field := strings.TrimPrefix(path, "$.")
	return data[field]
}

// GetFlow retrieves a flow definition
func (e *Engine) GetFlow(flowID string) (*FlowDefinition, bool) {
	e.flowsLock.RLock()
	defer e.flowsLock.RUnlock()
	flow, exists := e.flows[flowID]
	return flow, exists
}

// ListFlows returns all deployed flows
func (e *Engine) ListFlows() []*FlowDefinition {
	e.flowsLock.RLock()
	defer e.flowsLock.RUnlock()
	
	flows := make([]*FlowDefinition, 0, len(e.flows))
	for _, flow := range e.flows {
		flows = append(flows, flow)
	}
	return flows
}

