package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all CORS for UI
	},
}

// Client represents a connected WebSocket client.
type Client struct {
	hub         *WSHub
	conn        *websocket.Conn
	send        chan []byte
	executionID string // Filter for specific execution, or empty for all
}

// WSHub coordinates real-time WebSocket client connections and event broadcasting.
type WSHub struct {
	clients    map[*Client]bool
	broadcast  chan model.WorkflowEvent
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// NewWSHub creates a new WebSocket Hub.
func NewWSHub() *WSHub {
	return &WSHub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan model.WorkflowEvent, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the event loop for the WebSocket hub.
func (h *WSHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()

		case event := <-h.broadcast:
			bytes, err := json.Marshal(event)
			if err != nil {
				continue
			}

			h.mu.RLock()
			for client := range h.clients {
				// If client subscribed to specific execution ID, filter
				if client.executionID != "" && client.executionID != event.ExecutionID {
					continue
				}

				select {
				case client.send <- bytes:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends an event to all interested WebSocket clients.
func (h *WSHub) Broadcast(event model.WorkflowEvent) {
	select {
	case h.broadcast <- event:
	default:
		// Drop if buffer full
	}
}

// HandleWS upgrades HTTP connection to WebSocket and registers client.
func (h *WSHub) HandleWS(w http.ResponseWriter, r *http.Request, executionID string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade error", slog.String("error", err.Error()))
		return
	}

	client := &Client{
		hub:         h,
		conn:        conn,
		send:        make(chan []byte, 64),
		executionID: executionID,
	}

	h.register <- client

	// Start pump routines
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (c *Client) writePump() {
	defer c.conn.Close()

	for message := range c.send {
		w, err := c.conn.NextWriter(websocket.TextMessage)
		if err != nil {
			return
		}
		w.Write(message)
		if err := w.Close(); err != nil {
			return
		}
	}
}
