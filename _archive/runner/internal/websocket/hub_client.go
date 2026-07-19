package websocket

import (
	"encoding/json"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shift/runner/internal/logger"
)

// HubClient handles WebSocket connection to the Hub
type HubClient struct {
	conn     *websocket.Conn
	logger   *logger.Logger
	runnerID string
	send     chan Message
	onMessage func(Message)
}

// Message represents a WebSocket message
type Message struct {
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
}

// NewHubClient creates a new Hub WebSocket client
func NewHubClient(url, runnerID string, log *logger.Logger, onMessage func(Message)) *HubClient {
	return &HubClient{
		logger:    log,
		runnerID:  runnerID,
		send:      make(chan Message, 256),
		onMessage: onMessage,
	}
}

// Connect establishes connection to the Hub
func (c *HubClient) Connect(url string) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return err
	}

	c.conn = conn
	c.logger.Info("Connected to Hub WebSocket: %s", url)

	go c.writePump()
	go c.readPump()

	return nil
}

// Send sends a message to the Hub
func (c *HubClient) Send(msg Message) {
	select {
	case c.send <- msg:
	default:
		c.logger.Warn("Hub client send channel full, dropping message")
	}
}

// Close closes the connection
func (c *HubClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *HubClient) readPump() {
	defer c.conn.Close()

	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, rawMessage, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Error("Hub WebSocket error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(rawMessage, &msg); err != nil {
			c.logger.Error("Failed to unmarshal Hub message: %v", err)
			continue
		}

		c.logger.Info("Received message from Hub: %s", msg.Type)
		if c.onMessage != nil {
			c.onMessage(msg)
		}
	}
}

func (c *HubClient) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			json.NewEncoder(w).Encode(message)

			n := len(c.send)
			for i := 0; i < n; i++ {
				json.NewEncoder(w).Encode(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

