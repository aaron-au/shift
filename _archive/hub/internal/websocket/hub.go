package websocket

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shift/hub/internal/logger"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// Message represents a WebSocket message
type Message struct {
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
}

// Client represents a WebSocket client connection
type Client struct {
	ID       string
	Conn     *websocket.Conn
	Send     chan Message
	Hub      *Hub
	Logger   *logger.Logger
	IsRunner bool
}

// Hub maintains the set of active clients and broadcasts messages
type Hub struct {
	clients        map[*Client]bool
	broadcast      chan Message
	register       chan *Client
	unregister     chan *Client
	logger         *logger.Logger
	messageHandler func(Message)
	mu             sync.RWMutex
}

// NewHub creates a new Hub instance
func NewHub(log *logger.Logger) *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		logger:     log,
	}
}

// SetMessageHandler sets a callback for processing incoming messages
func (h *Hub) SetMessageHandler(handler func(Message)) {
	h.messageHandler = handler
}

// Run starts the hub's message handling loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.logger.Info("Client registered: %s (Total clients: %d)", client.ID, len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
			}
			h.mu.Unlock()
			h.logger.Info("Client unregistered: %s (Total clients: %d)", client.ID, len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends a message to all connected clients
func (h *Hub) Broadcast(msg Message) {
	h.broadcast <- msg
}

// SendToRunner sends a message to a specific runner
func (h *Hub) SendToRunner(runnerID string, msg Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	found := false
	for client := range h.clients {
		if client.IsRunner && client.ID == runnerID {
			found = true
			select {
			case client.Send <- msg:
				h.logger.Info("Sent message %s to runner %s (client found)", msg.Type, runnerID)
			default:
				h.logger.Warn("Runner %s send channel full, closing connection", runnerID)
				close(client.Send)
				delete(h.clients, client)
			}
			return
		}
	}
	
	if !found {
		h.logger.Warn("Runner %s not found in connected clients (total: %d)", runnerID, len(h.clients))
		// Log all connected runner IDs for debugging
		runnerIDs := make([]string, 0)
		for client := range h.clients {
			if client.IsRunner {
				runnerIDs = append(runnerIDs, client.ID)
			}
		}
		h.logger.Info("Connected runners: %v", runnerIDs)
	}
}

// GetRunners returns a list of all connected runner IDs
func (h *Hub) GetRunners() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	runners := make([]string, 0)
	for client := range h.clients {
		if client.IsRunner {
			runners = append(runners, client.ID)
		}
	}
	return runners
}

// HandleWebSocket handles WebSocket connections
func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request, clientID string, isRunner bool) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade connection: %v", err)
		return
	}

	client := &Client{
		ID:       clientID,
		Conn:     conn,
		Send:     make(chan Message, 256),
		Hub:      h,
		Logger:   h.logger,
		IsRunner: isRunner,
	}

	client.Hub.register <- client

	go client.writePump()
	go client.readPump()
}

// readPump pumps messages from the websocket connection to the hub
func (c *Client) readPump() {
	defer func() {
		c.Hub.unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, rawMessage, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.Logger.Error("WebSocket error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(rawMessage, &msg); err != nil {
			c.Logger.Error("Failed to unmarshal message: %v", err)
			continue
		}

		msg.From = c.ID
		msg.Timestamp = time.Now()

		// Process message with handler if set
		if c.Hub.messageHandler != nil {
			c.Hub.messageHandler(msg)
		}

		// Broadcast to all clients (including UI)
		c.Hub.Broadcast(msg)
		c.Logger.Info("Received message from %s: %s", c.ID, msg.Type)
	}
}

// writePump pumps messages from the hub to the websocket connection
func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Send each message separately to avoid JSON concatenation issues
			// The gorilla/websocket library handles framing, so we send one message per frame
			if err := c.Conn.WriteJSON(message); err != nil {
				return
			}
			
			// Send any additional buffered messages
			n := len(c.Send)
			for i := 0; i < n; i++ {
				msg := <-c.Send
				if err := c.Conn.WriteJSON(msg); err != nil {
					return
				}
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

