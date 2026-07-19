package websocket

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shift/runner/internal/logger"
)

var p2pUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// P2PClient represents a peer-to-peer WebSocket client
type P2PClient struct {
	ID       string
	Conn     *websocket.Conn
	Send     chan Message
	Hub      *P2PHub
	Logger   *logger.Logger
}

// P2PHub maintains P2P connections between runners
type P2PHub struct {
	clients       map[*P2PClient]bool
	broadcast     chan Message
	register      chan *P2PClient
	unregister    chan *P2PClient
	logger        *logger.Logger
	onMessage     func(Message) // Callback for forwarding messages to Hub
	mu            sync.RWMutex
}

// NewP2PHub creates a new P2P Hub instance
func NewP2PHub(log *logger.Logger, onMessage func(Message)) *P2PHub {
	return &P2PHub{
		clients:    make(map[*P2PClient]bool),
		broadcast:  make(chan Message),
		register:   make(chan *P2PClient),
		unregister: make(chan *P2PClient),
		logger:     log,
		onMessage:  onMessage,
	}
}

// Run starts the P2P hub's message handling loop
func (h *P2PHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.logger.Info("P2P client registered: %s (Total peers: %d)", client.ID, len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
			}
			h.mu.Unlock()
			h.logger.Info("P2P client unregistered: %s (Total peers: %d)", client.ID, len(h.clients))

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

// Broadcast sends a message to all connected peers
func (h *P2PHub) Broadcast(msg Message) {
	h.broadcast <- msg
}

// GetBroadcastChannel returns the broadcast channel for external use
func (h *P2PHub) GetBroadcastChannel() chan Message {
	return h.broadcast
}

// HandleP2PWebSocket handles P2P WebSocket connections
func (h *P2PHub) HandleP2PWebSocket(w http.ResponseWriter, r *http.Request, peerID string) {
	conn, err := p2pUpgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade P2P connection: %v", err)
		return
	}

	client := &P2PClient{
		ID:     peerID,
		Conn:   conn,
		Send:   make(chan Message, 256),
		Hub:    h,
		Logger: h.logger,
	}

	client.Hub.register <- client

	go client.writePump()
	go client.readPump()
}

func (c *P2PClient) readPump() {
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
				c.Logger.Error("P2P WebSocket error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(rawMessage, &msg); err != nil {
			c.Logger.Error("Failed to unmarshal P2P message: %v", err)
			continue
		}

		msg.From = c.ID
		msg.Timestamp = time.Now()

		c.Logger.Info("Received P2P message from %s: %s", c.ID, msg.Type)
		
		// Forward to Hub if callback is set
		if c.Hub.onMessage != nil {
			c.Hub.onMessage(msg)
		}
		
		c.Hub.Broadcast(msg)
	}
}

func (c *P2PClient) writePump() {
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

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			json.NewEncoder(w).Encode(message)

			n := len(c.Send)
			for i := 0; i < n; i++ {
				json.NewEncoder(w).Encode(<-c.Send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

