package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shift/runner/internal/logger"
)

// HTTPConnector implements the Connector interface for HTTP operations
type HTTPConnector struct {
	client  *http.Client
	logger  *logger.Logger
	config  map[string]interface{}
}

// NewHTTPConnector creates a new HTTP connector
func NewHTTPConnector(log *logger.Logger) *HTTPConnector {
	return &HTTPConnector{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: log,
	}
}

// Connect initializes the HTTP connector with configuration
func (h *HTTPConnector) Connect(ctx context.Context, config map[string]interface{}) error {
	h.config = config
	
	// Configure HTTP client timeout if specified
	if timeout, ok := config["timeout_seconds"].(float64); ok {
		h.client.Timeout = time.Duration(timeout) * time.Second
	}
	
	h.logger.Info("HTTP connector connected")
	return nil
}

// Execute performs an HTTP operation
func (h *HTTPConnector) Execute(ctx context.Context, action string, input map[string]interface{}) (map[string]interface{}, error) {
	// Extract URL
	urlStr, ok := input["url"].(string)
	if !ok {
		return nil, fmt.Errorf("url is required")
	}
	
	// Parse URL
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	
	// Add query parameters if provided
	if params, ok := input["params"].(map[string]interface{}); ok {
		query := parsedURL.Query()
		for key, value := range params {
			query.Set(key, fmt.Sprintf("%v", value))
		}
		parsedURL.RawQuery = query.Encode()
	}
	
	// Create request based on action
	var req *http.Request
	var body io.Reader
	
	// Handle request body for POST, PUT, PATCH
	if action == "POST" || action == "PUT" || action == "PATCH" {
		if bodyData, ok := input["body"]; ok {
			bodyBytes, err := json.Marshal(bodyData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal body: %w", err)
			}
			body = bytes.NewBuffer(bodyBytes)
		}
	}
	
	req, err = http.NewRequestWithContext(ctx, action, parsedURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Set headers
	if headers, ok := input["headers"].(map[string]interface{}); ok {
		for key, value := range headers {
			req.Header.Set(key, fmt.Sprintf("%v", value))
		}
	}
	
	// Set default Content-Type for POST/PUT/PATCH if not specified
	if (action == "POST" || action == "PUT" || action == "PATCH") && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	
	// Handle authentication
	if auth, ok := input["auth"].(map[string]interface{}); ok {
		if authType, ok := auth["type"].(string); ok {
			switch strings.ToUpper(authType) {
			case "BASIC":
				if username, ok := auth["username"].(string); ok {
					if password, ok := auth["password"].(string); ok {
						req.SetBasicAuth(username, password)
					}
				}
			case "BEARER":
				if token, ok := auth["token"].(string); ok {
					req.Header.Set("Authorization", "Bearer "+token)
				}
			case "API_KEY":
				if value, ok := auth["value"].(string); ok {
					headerName := "X-API-Key"
					if name, ok := auth["header_name"].(string); ok && name != "" {
						headerName = name
					}
					req.Header.Set(headerName, value)
				}
			}
		}
	}
	
	// Execute request
	h.logger.Info("Executing HTTP %s to %s", action, parsedURL.String())
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()
	
	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	// Try to parse as JSON, fallback to string
	var responseData interface{}
	if err := json.Unmarshal(respBody, &responseData); err != nil {
		responseData = string(respBody)
	}
	
	result := map[string]interface{}{
		"status_code": resp.StatusCode,
		"status":      resp.Status,
		"headers":     resp.Header,
		"body":        responseData,
	}
	
	h.logger.Info("HTTP %s completed with status %d", action, resp.StatusCode)
	return result, nil
}

// Shutdown gracefully terminates the HTTP connector
func (h *HTTPConnector) Shutdown(ctx context.Context) error {
	h.logger.Info("HTTP connector shutdown")
	return nil
}

