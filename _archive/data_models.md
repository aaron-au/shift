# SHIFT: Core Data Models

This document defines the primary data structures for the SHIFT platform.

## Hub Models (PostgreSQL)

### RunnerGroup

Represents a logical cluster of runners for a specific account.

```go
type RunnerGroup struct {
    ID          string    `db:"id"`         // Unique identifier (UUID)
    AccountID   string    `db:"account_id"` // Belongs to which account
    Name        string    `db:"name"`       // User-defined name (e.g., "Production Cluster")
    CreatedAt   time.Time `db:"created_at"`
    UpdatedAt   time.Time `db:"updated_at"`
}
```

## Runner Models (SQLite)

### DistributedTask

Represents an integration flow execution task in the shared queue.

```go
type DistributedTask struct {
    TaskID            string    `db:"task_id"`              // Unique identifier (UUID)
    FlowID            string    `db:"flow_id"`              // Which flow to execute
    Status            string    `db:"status"`               // PENDING, CLAIMED, COMPLETED, FAILED
    Payload           []byte    `db:"payload"`              // JSON payload for the flow
    ClaimedByRunnerID *string   `db:"claimed_by_runner_id"` // Which runner is executing it
    CreatedAt         time.Time `db:"created_at"`
    ClaimedAt         *time.Time`db:"claimed_at"`
}
```