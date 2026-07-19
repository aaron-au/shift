# SHIFT: API & Communication Contracts

## 1. Hub-to-Runner WebSocket Messages (JSON)

### Message: DeployFlow

*   **Direction:** Hub -> Runner
*   **Purpose:** Instructs the runner to add or update an integration flow.

```json
{
  "type": "DeployFlow",
  "payload": {
    "flowId": "flow-uuid-123",
    "version": 3,
    "definition": {
      // ... full flow definition object
    }
  }
}
```

## 2. Runner-to-Runner WebSocket Messages (JSON)

### Message: NewTaskNotification

*   **Direction:** Runner -> Peers
*   **Purpose:** Notifies peers that a new task is available in the distributed queue.

```json
{
  "type": "NewTaskNotification",
  "payload": {
    "taskId": "task-uuid-456"
  }
}
```

## 3. Connector RPC Interface (Go)

This is the Go interface that all connector plugins must implement.

```go
// Connector is the interface for all SHIFT integration plugins.
type Connector interface {
    // Connect initializes the connector with its configuration.
    Connect(ctx context.Context, configJSON []byte) error

    // Execute performs a specific action.
    Execute(ctx context.Context, actionID string, inputJSON []byte) (outputJSON []byte, err error)

    // Shutdown gracefully terminates the connector.
    Shutdown(ctx context.Context) error
}
```