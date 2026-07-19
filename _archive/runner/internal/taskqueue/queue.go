package taskqueue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/shift/runner/internal/logger"
)

// Task represents an integration execution task
type Task struct {
	ID          string
	FlowID      string
	AccountID   string
	Status      string // pending, accepted, running, completed, failed
	InputPayload json.RawMessage
	OutputPayload json.RawMessage
	ErrorMessage string
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// Queue manages the task queue using SQLite
type Queue struct {
	db     *sql.DB
	logger *logger.Logger
}

// NewQueue creates a new task queue
func NewQueue(dbPath string, log *logger.Logger) (*Queue, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	q := &Queue{
		db:     db,
		logger: log,
	}

	if err := q.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return q, nil
}

// initSchema creates the task queue tables
func (q *Queue) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		flow_id TEXT NOT NULL,
		account_id TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'running', 'completed', 'failed')),
		input_payload TEXT,
		output_payload TEXT,
		error_message TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		started_at TIMESTAMP,
		completed_at TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
	CREATE INDEX IF NOT EXISTS idx_tasks_flow_id ON tasks(flow_id);
	CREATE INDEX IF NOT EXISTS idx_tasks_account_id ON tasks(account_id);
	CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
	`

	_, err := q.db.Exec(schema)
	return err
}

// AddTask adds a new task to the queue
// Returns nil if task already exists (for scheduled task coordination)
func (q *Queue) AddTask(task *Task) error {
	inputJSON, _ := json.Marshal(task.InputPayload)
	
	query := `
		INSERT INTO tasks (id, flow_id, account_id, status, input_payload, created_at)
		VALUES (?, ?, ?, 'pending', ?, ?)
	`
	
	_, err := q.db.Exec(query, task.ID, task.FlowID, task.AccountID, string(inputJSON), task.CreatedAt)
	if err != nil {
		// Check if it's a unique constraint violation (task already exists)
		if err.Error() == "UNIQUE constraint failed: tasks.id" || 
		   err.Error() == "constraint failed: UNIQUE constraint failed: tasks.id" {
			return fmt.Errorf("task already exists")
		}
		return fmt.Errorf("failed to add task: %w", err)
	}
	
	q.logger.Info("Added task %s for flow %s", task.ID, task.FlowID)
	return nil
}

// ClaimTask atomically claims a pending task
func (q *Queue) ClaimTask(runnerID string) (*Task, error) {
	tx, err := q.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Atomic claim: UPDATE ... WHERE status = 'pending'
	query := `
		UPDATE tasks
		SET status = 'accepted', started_at = ?
		WHERE id = (
			SELECT id FROM tasks
			WHERE status = 'pending'
			ORDER BY created_at ASC
			LIMIT 1
		)
		RETURNING id, flow_id, account_id, status, input_payload, output_payload, error_message, created_at, started_at, completed_at
	`

	now := time.Now()
	var task Task
	var inputPayload, outputPayload, errorMessage sql.NullString
	var startedAt, completedAt sql.NullTime

	err = tx.QueryRow(query, now).Scan(
		&task.ID, &task.FlowID, &task.AccountID, &task.Status,
		&inputPayload, &outputPayload, &errorMessage,
		&task.CreatedAt, &startedAt, &completedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // No pending tasks
	}
	if err != nil {
		return nil, fmt.Errorf("failed to claim task: %w", err)
	}

	if inputPayload.Valid {
		json.Unmarshal([]byte(inputPayload.String), &task.InputPayload)
	}
	if outputPayload.Valid {
		json.Unmarshal([]byte(outputPayload.String), &task.OutputPayload)
	}
	if errorMessage.Valid {
		task.ErrorMessage = errorMessage.String
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	q.logger.Info("Claimed task %s", task.ID)
	return &task, nil
}

// UpdateTaskStatus updates the status of a task
func (q *Queue) UpdateTaskStatus(taskID, status string, outputPayload json.RawMessage, errorMessage string) error {
	var query string
	now := time.Now()

	if status == "completed" || status == "failed" {
		outputJSON, _ := json.Marshal(outputPayload)
		query = `
			UPDATE tasks
			SET status = ?, output_payload = ?, error_message = ?, completed_at = ?
			WHERE id = ?
		`
		_, err := q.db.Exec(query, status, string(outputJSON), errorMessage, now, taskID)
		return err
	} else if status == "running" {
		query = `
			UPDATE tasks
			SET status = ?
			WHERE id = ?
		`
		_, err := q.db.Exec(query, status, taskID)
		return err
	}

	return fmt.Errorf("invalid status: %s", status)
}

// GetTask retrieves a task by ID
func (q *Queue) GetTask(taskID string) (*Task, error) {
	query := `
		SELECT id, flow_id, account_id, status, input_payload, output_payload, error_message, created_at, started_at, completed_at
		FROM tasks
		WHERE id = ?
	`

	var task Task
	var inputPayload, outputPayload, errorMessage sql.NullString
	var startedAt, completedAt sql.NullTime

	err := q.db.QueryRow(query, taskID).Scan(
		&task.ID, &task.FlowID, &task.AccountID, &task.Status,
		&inputPayload, &outputPayload, &errorMessage,
		&task.CreatedAt, &startedAt, &completedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if inputPayload.Valid {
		json.Unmarshal([]byte(inputPayload.String), &task.InputPayload)
	}
	if outputPayload.Valid {
		json.Unmarshal([]byte(outputPayload.String), &task.OutputPayload)
	}
	if errorMessage.Valid {
		task.ErrorMessage = errorMessage.String
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}

	return &task, nil
}

// GetPendingTasks retrieves all pending tasks (for debugging/stuck task detection)
func (q *Queue) GetPendingTasks() ([]*Task, error) {
	query := `
		SELECT id, flow_id, account_id, status, input_payload, output_payload, error_message, created_at, started_at, completed_at
		FROM tasks
		WHERE status = 'pending'
		ORDER BY created_at ASC
	`

	rows, err := q.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var task Task
		var inputPayload, outputPayload, errorMessage sql.NullString
		var startedAt, completedAt sql.NullTime

		err := rows.Scan(
			&task.ID, &task.FlowID, &task.AccountID, &task.Status,
			&inputPayload, &outputPayload, &errorMessage,
			&task.CreatedAt, &startedAt, &completedAt,
		)
		if err != nil {
			continue
		}

		if inputPayload.Valid {
			json.Unmarshal([]byte(inputPayload.String), &task.InputPayload)
		}
		if outputPayload.Valid {
			json.Unmarshal([]byte(outputPayload.String), &task.OutputPayload)
		}
		if errorMessage.Valid {
			task.ErrorMessage = errorMessage.String
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}

		tasks = append(tasks, &task)
	}

	return tasks, nil
}

// Close closes the database connection
func (q *Queue) Close() error {
	return q.db.Close()
}

