// Package p2papi provides HTTP API endpoints for P2P broker management
package p2papi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/p2pbroker"
	"github.com/rs/zerolog/log"
)

// Handler handles P2P API requests
type Handler struct {
	broker *p2pbroker.P2PBroker
}

// NewHandler creates a new P2P API handler
func NewHandler(broker *p2pbroker.P2PBroker) *Handler {
	return &Handler{
		broker: broker,
	}
}

// Routes returns the P2P API routes (require API key authentication)
func (h *Handler) Routes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/status":  h.handleStatus,  // Admin: detailed P2P status
		"/peers":   h.handlePeers,   // Admin: P2P peer info
		"/ping":    h.handlePing,    // Admin: server-to-server ping
		"/addrs":   h.handleAddrs,   // Admin: P2P multiaddresses
		"/refresh": h.handleRefresh, // Admin: trigger node discovery refresh
	}
}

// ClientRoutes returns routes that don't require API key authentication
// These are meant for frontend clients to use directly
func (h *Handler) ClientRoutes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/nodes":   h.handleNodes,   // Frontend: get all available Centrifugo nodes
		"/latency": h.handleLatency, // Frontend: ping endpoint for RTT measurement
	}
}

// Response represents a generic API response
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func (h *Handler) writeJSON(w http.ResponseWriter, statusCode int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error().Err(err).Msg("failed to encode P2P API response")
	}
}

func (h *Handler) writeSuccess(w http.ResponseWriter, data interface{}) {
	h.writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    data,
	})
}

func (h *Handler) writeError(w http.ResponseWriter, statusCode int, errMsg string) {
	h.writeJSON(w, statusCode, Response{
		Success: false,
		Error:   errMsg,
	})
}

// handleStatus returns the full P2P node status
// GET /p2p/status
func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	status := h.broker.GetNodeStatus()
	h.writeSuccess(w, status)
}

// handlePeers returns detailed information about connected peers
// GET /p2p/peers
func (h *Handler) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	peers := h.broker.GetPeersDetailed()
	h.writeSuccess(w, map[string]interface{}{
		"peer_count": len(peers),
		"peers":      peers,
	})
}

// PingRequest represents a ping request
type PingRequest struct {
	PeerID string `json:"peer_id"`
}

// handlePing pings one or all peers and returns latency information
// GET /p2p/ping - ping all peers
// POST /p2p/ping - ping specific peer (body: {"peer_id": "..."})
func (h *Handler) handlePing(w http.ResponseWriter, r *http.Request) {
	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Ping all peers
		results := h.broker.PingAllPeers()
		h.writeSuccess(w, map[string]interface{}{
			"results": results,
		})

	case http.MethodPost:
		// Ping specific peer
		var req PingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		if req.PeerID == "" {
			h.writeError(w, http.StatusBadRequest, "peer_id is required")
			return
		}

		result := h.broker.PingPeer(req.PeerID)
		h.writeSuccess(w, result)

	default:
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAddrs returns the multiaddresses of this node
// GET /p2p/addrs
func (h *Handler) handleAddrs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	addrs := h.broker.GetMultiaddrs()
	h.writeSuccess(w, map[string]interface{}{
		"peer_id":    h.broker.GetPeerID(),
		"multiaddrs": addrs,
	})
}

// handleNodes returns all available Centrifugo nodes for frontend clients
// GET /p2p/nodes
// This is the main API for frontend to discover available Centrifugo servers
// NO AUTHENTICATION REQUIRED - this is public for clients
func (h *Handler) handleNodes(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers for browser access
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	nodes := h.broker.GetCentrifugoNodes()

	// Build response with connection URLs
	type NodeResponse struct {
		PeerID    string `json:"peer_id"`
		IP        string `json:"ip"`
		Port      int    `json:"port"`
		TLS       bool   `json:"tls"`
		WebSocket string `json:"websocket"`
		HTTP      string `json:"http"`
		LastSeen  int64  `json:"last_seen"`
	}

	nodeResponses := make([]NodeResponse, 0, len(nodes))
	for _, node := range nodes {
		scheme := "http"
		if node.TLS {
			scheme = "https"
		}
		nodeResponses = append(nodeResponses, NodeResponse{
			PeerID:    node.PeerID,
			IP:        node.IP,
			Port:      node.Port,
			TLS:       node.TLS,
			WebSocket: node.WebSocket,
			HTTP:      scheme + "://" + node.IP + ":" + strconv.Itoa(node.Port),
			LastSeen:  node.LastSeen,
		})
	}

	h.writeSuccess(w, map[string]interface{}{
		"node_count": len(nodeResponses),
		"nodes":      nodeResponses,
	})
}

// handleRefresh triggers a refresh of node discovery
// POST /p2p/refresh
func (h *Handler) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.broker == nil {
		h.writeError(w, http.StatusServiceUnavailable, "P2P broker not available")
		return
	}

	// Cleanup offline nodes first
	removed := h.broker.CleanupOfflineNodes()

	// Request fresh info from peers
	h.broker.RefreshNodes()

	h.writeSuccess(w, map[string]interface{}{
		"message":       "Node refresh requested",
		"nodes_removed": removed,
	})
}

// handleLatency is a lightweight endpoint for client-side latency testing
// GET /p2p/latency
// This endpoint is designed for frontend clients to measure RTT to this server
// It returns minimal data to minimize network overhead
// NO AUTHENTICATION REQUIRED - this is public for clients to test
func (h *Handler) handleLatency(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers for browser access
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Return minimal response with server timestamp
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Include peer_id so client knows which node responded
	peerID := ""
	if h.broker != nil {
		peerID = h.broker.GetPeerID()
	}

	// Minimal JSON response
	response := struct {
		OK     bool   `json:"ok"`
		T      int64  `json:"t"` // Server timestamp in milliseconds
		PeerID string `json:"peer_id,omitempty"`
	}{
		OK:     true,
		T:      timeNowMs(),
		PeerID: peerID,
	}

	_ = json.NewEncoder(w).Encode(response)
}

func timeNowMs() int64 {
	return time.Now().UnixMilli()
}
