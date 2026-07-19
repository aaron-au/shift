package docker

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/shift/hub/internal/logger"
)

// ContainerInfo represents container information
type ContainerInfo struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Client wraps Docker client for log streaming (using docker CLI)
type Client struct {
	logger *logger.Logger
}

// NewClient creates a new Docker client
func NewClient(log *logger.Logger) (*Client, error) {
	// Verify docker is available
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("docker command not found: %w", err)
	}
	
	return &Client{
		logger: log,
	}, nil
}

// StreamLogs streams logs from a container using docker logs command
func (c *Client) StreamLogs(ctx context.Context, containerName string, since time.Time, follow bool) (string, error) {
	args := []string{"logs", "--timestamps"}
	
	// Add since if specified
	if !since.IsZero() {
		args = append(args, "--since", since.Format(time.RFC3339))
	}
	
	// Add tail
	args = append(args, "--tail", "100")
	
	// Note: follow mode doesn't work well with exec.Command for streaming
	// For now, we'll just get the last 100 lines
	// In production, you'd want to use the Docker API or a proper streaming approach
	
	args = append(args, containerName)
	
	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if it's just because container doesn't exist or no logs yet
		if strings.Contains(string(output), "No such container") {
			return "", fmt.Errorf("container not found: %s", containerName)
		}
		// For empty logs, return empty string instead of error
		if len(output) == 0 {
			return "", nil
		}
		return "", fmt.Errorf("docker logs failed: %w, output: %s", err, string(output))
	}
	
	return string(output), nil
}

// ListContainers lists all containers using docker ps
func (c *Client) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	cmd := exec.CommandContext(ctx, "docker", "ps", "-a", "--format", "{{.ID}}\t{{.Names}}\t{{.Status}}")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps failed: %w", err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	containers := make([]ContainerInfo, 0)
	
	for _, line := range lines {
		if line == "" {
			continue
		}
		
		parts := strings.Split(line, "\t")
		if len(parts) >= 3 {
			name := parts[1]
			// Filter to SHIFT containers
			if strings.Contains(name, "shift-") {
				containers = append(containers, ContainerInfo{
					ID:     parts[0][:12], // First 12 chars
					Name:   name,
					Status: parts[2],
				})
			}
		}
	}
	
	return containers, nil
}

