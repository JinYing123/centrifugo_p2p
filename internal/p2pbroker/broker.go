// Package p2pbroker implements a P2P Broker for Centrifuge using libp2p and GossipSub.
package p2pbroker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog/log"
)

// Config P2P Broker configuration
type Config struct {
	// Enabled enables P2P broker
	Enabled bool `json:"enabled" mapstructure:"enabled"`
	// ListenPort is the port to listen for P2P connections
	ListenPort int `json:"listen_port" mapstructure:"listen_port"`
	// AdvertiseIP is the public IP to advertise (empty for auto-detection)
	AdvertiseIP string `json:"advertise_ip" mapstructure:"advertise_ip"`
	// BootstrapNodes is a list of bootstrap node multiaddresses
	BootstrapNodes []string `json:"bootstrap_nodes" mapstructure:"bootstrap_nodes"`
	// TopicPrefix is prefix for all pubsub topics
	TopicPrefix string `json:"topic_prefix" mapstructure:"topic_prefix"`
	// EnableMDNS enables local network discovery via mDNS
	EnableMDNS bool `json:"enable_mdns" mapstructure:"enable_mdns"`
	// IdentityKeyFile is the path to save/load the node's private key for fixed Peer ID
	// If empty, a new key is generated on each start (Peer ID will change)
	IdentityKeyFile string `json:"identity_key_file" mapstructure:"identity_key_file"`
	// CentrifugoPort is the HTTP/WebSocket port of Centrifugo (for client connections)
	CentrifugoPort int `json:"centrifugo_port" mapstructure:"centrifugo_port"`
	// CentrifugoTLS indicates if Centrifugo uses TLS (wss:// vs ws://)
	CentrifugoTLS bool `json:"centrifugo_tls" mapstructure:"centrifugo_tls"`
}

// DefaultConfig returns default P2P broker configuration
func DefaultConfig() Config {
	return Config{
		Enabled:        false,
		ListenPort:     4001,
		AdvertiseIP:    "",
		BootstrapNodes: []string{},
		TopicPrefix:    "centrifugo",
		EnableMDNS:     true,
	}
}

type topicWrapper struct {
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	origChannel  string
}

// CentrifugoNodeInfo represents connection info for a Centrifugo node
type CentrifugoNodeInfo struct {
	PeerID    string `json:"peer_id"`
	IP        string `json:"ip"`
	Port      int    `json:"port"`
	TLS       bool   `json:"tls"`
	WebSocket string `json:"websocket"` // Full WebSocket URL: ws://ip:port/connection/websocket
	LastSeen  int64  `json:"last_seen"` // Unix timestamp
}

// P2PBroker is a broker on top of libp2p and GossipSub.
type P2PBroker struct {
	node   *centrifuge.Node
	config Config

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	startErr  error

	host   host.Host
	pubsub *pubsub.PubSub
	dht    *dht.IpfsDHT

	topicsMu sync.RWMutex
	topics   map[string]*topicWrapper

	// Cache for node-specific topics (used by PublishControl)
	nodeTopicsMu sync.RWMutex
	nodeTopics   map[string]*pubsub.Topic

	controlTopic *pubsub.Topic
	controlSub   *pubsub.Subscription
	nodeTopic    *pubsub.Topic
	nodeSub      *pubsub.Subscription

	// Discovery topic for exchanging Centrifugo node info
	discoveryTopic *pubsub.Topic
	discoverySub   *pubsub.Subscription

	// Known Centrifugo nodes (peer ID -> node info)
	knownNodesMu sync.RWMutex
	knownNodes   map[string]*CentrifugoNodeInfo

	eventHandler        centrifuge.BrokerEventHandler
	controlEventHandler centrifuge.ControlEventHandler
}

var _ centrifuge.Broker = (*P2PBroker)(nil)

// New creates a new P2PBroker
func New(n *centrifuge.Node, cfg Config) (*P2PBroker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	b := &P2PBroker{
		node:       n,
		config:     cfg,
		ctx:        ctx,
		cancel:     cancel,
		topics:     make(map[string]*topicWrapper),
		nodeTopics: make(map[string]*pubsub.Topic),
		knownNodes: make(map[string]*CentrifugoNodeInfo),
	}

	return b, nil
}

// ensureStarted ensures the P2P network is initialized (only once)
func (b *P2PBroker) ensureStarted() error {
	b.startOnce.Do(func() {
		b.startErr = b.start()
	})
	return b.startErr
}

// loadOrGenerateIdentity loads identity from file or generates a new one
func (b *P2PBroker) loadOrGenerateIdentity() (crypto.PrivKey, error) {
	// If no identity file configured, generate a new key each time
	if b.config.IdentityKeyFile == "" {
		log.Info().Msg("No identity key file configured, generating ephemeral key (Peer ID will change on restart)")
		priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, -1, rand.Reader)
		return priv, err
	}

	keyFile := b.config.IdentityKeyFile

	// Ensure directory exists
	dir := filepath.Dir(keyFile)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory for identity key: %w", err)
		}
	}

	// Try to load existing key
	if _, err := os.Stat(keyFile); err == nil {
		// File exists, load it
		data, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read identity key file: %w", err)
		}

		keyBytes, err := hex.DecodeString(strings.TrimSpace(string(data)))
		if err != nil {
			return nil, fmt.Errorf("failed to decode identity key: %w", err)
		}

		priv, err := crypto.UnmarshalPrivateKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal identity key: %w", err)
		}

		// Get peer ID from key
		peerID, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return nil, fmt.Errorf("failed to get peer ID from key: %w", err)
		}

		log.Info().
			Str("key_file", keyFile).
			Str("peer_id", peerID.String()).
			Msg("Loaded existing identity key (Peer ID is fixed)")

		return priv, nil
	}

	// File doesn't exist, generate new key and save it
	log.Info().Str("key_file", keyFile).Msg("Generating new identity key...")

	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, -1, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Marshal and save the key
	keyBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	keyHex := hex.EncodeToString(keyBytes)
	if err := os.WriteFile(keyFile, []byte(keyHex), 0600); err != nil {
		return nil, fmt.Errorf("failed to save identity key: %w", err)
	}

	// Get peer ID
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer ID: %w", err)
	}

	log.Info().
		Str("key_file", keyFile).
		Str("peer_id", peerID.String()).
		Msg("Generated and saved new identity key (Peer ID is now fixed)")

	return priv, nil
}

// start initializes the P2P network
func (b *P2PBroker) start() error {
	log.Info().Msg("Starting P2P Broker...")

	// Load or generate identity
	priv, err := b.loadOrGenerateIdentity()
	if err != nil {
		return fmt.Errorf("failed to load/generate identity: %w", err)
	}

	// Determine listen addresses
	listenAddrs := []string{
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", b.config.ListenPort),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", b.config.ListenPort),
	}

	// Create libp2p host
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableNATService(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	}

	// Add external address if configured
	if b.config.AdvertiseIP != "" {
		extAddr := fmt.Sprintf("/ip4/%s/tcp/%d", b.config.AdvertiseIP, b.config.ListenPort)
		opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			extMultiAddr, err := multiaddr.NewMultiaddr(extAddr)
			if err == nil {
				addrs = append(addrs, extMultiAddr)
			}
			return addrs
		}))
	} else {
		// Try to get public IP
		if publicIP := getPublicIP(); publicIP != "" {
			extAddr := fmt.Sprintf("/ip4/%s/tcp/%d", publicIP, b.config.ListenPort)
			opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
				extMultiAddr, err := multiaddr.NewMultiaddr(extAddr)
				if err == nil {
					addrs = append(addrs, extMultiAddr)
				}
				return addrs
			}))
			log.Info().Str("public_ip", publicIP).Msg("Auto-detected public IP")
		}
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return fmt.Errorf("failed to create libp2p host: %w", err)
	}
	b.host = h

	log.Info().
		Str("peer_id", h.ID().String()).
		Strs("listen_addrs", listenAddrsToStrings(h.Addrs())).
		Msg("P2P host created")

	// Create DHT for peer discovery
	b.dht, err = dht.New(b.ctx, h, dht.Mode(dht.ModeAutoServer))
	if err != nil {
		return fmt.Errorf("failed to create DHT: %w", err)
	}

	if err := b.dht.Bootstrap(b.ctx); err != nil {
		return fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	// Connect to bootstrap nodes
	if len(b.config.BootstrapNodes) > 0 {
		go b.connectToBootstrapNodes()
	}

	// Enable mDNS for local discovery
	if b.config.EnableMDNS {
		mdnsService := mdns.NewMdnsService(h, "centrifugo-p2p", &mdnsNotifee{broker: b})
		if err := mdnsService.Start(); err != nil {
			log.Warn().Err(err).Msg("Failed to start mDNS service")
		} else {
			log.Info().Msg("mDNS discovery enabled")
		}
	}

	// Start DHT-based discovery
	go b.discoverPeers()

	// Create GossipSub
	b.pubsub, err = pubsub.NewGossipSub(b.ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true),
	)
	if err != nil {
		return fmt.Errorf("failed to create GossipSub: %w", err)
	}

	log.Info().Msg("GossipSub initialized")

	// Setup discovery topic for exchanging Centrifugo node info
	if err := b.setupDiscoveryTopic(); err != nil {
		log.Warn().Err(err).Msg("Failed to setup discovery topic")
	}

	// Start bootstrap node reconnection loop
	go b.bootstrapReconnectLoop()

	return nil
}

// bootstrapReconnectLoop periodically checks and reconnects to bootstrap nodes
func (b *P2PBroker) bootstrapReconnectLoop() {
	if len(b.config.BootstrapNodes) == 0 {
		return
	}

	// Initial delay
	time.Sleep(10 * time.Second)

	// Check less frequently - every 2 minutes
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.checkAndReconnectBootstrapNodes()
		}
	}
}

// checkAndReconnectBootstrapNodes checks bootstrap node connections and reconnects if needed
func (b *P2PBroker) checkAndReconnectBootstrapNodes() {
	if b.host == nil {
		return
	}

	connectedPeers := len(b.host.Network().Peers())

	// If we have no peers, try to reconnect to bootstrap nodes
	if connectedPeers == 0 {
		log.Info().Msg("No connected peers, attempting to reconnect to bootstrap nodes...")
		b.connectToBootstrapNodes()
		return
	}

	// Check each bootstrap node and reconnect only if disconnected
	for _, addr := range b.config.BootstrapNodes {
		go b.tryReconnectBootstrapNode(addr)
	}
}

// tryReconnectBootstrapNode attempts to reconnect to a bootstrap node if not connected
func (b *P2PBroker) tryReconnectBootstrapNode(addr string) {
	if b.host == nil {
		return
	}

	// Parse the address to get peer ID
	var maddr multiaddr.Multiaddr
	var err error

	if strings.HasPrefix(addr, "/") {
		maddr, err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return
		}
	} else {
		// Simple IP:PORT format - can't check connection status without peer ID
		// Skip reconnection check for addresses without peer ID
		return
	}

	// Try to extract peer ID from multiaddr
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		// No peer ID in address, can't check connection status
		return
	}

	// Check connection status - use both Connectedness and peer list
	connectedness := b.host.Network().Connectedness(peerInfo.ID)
	connectedPeers := b.host.Network().Peers()

	// Check if peer is actually in our connected peers list
	isInPeerList := false
	for _, p := range connectedPeers {
		if p == peerInfo.ID {
			isInPeerList = true
			break
		}
	}

	// Only reconnect if:
	// 1. Connectedness says not connected (0), OR
	// 2. Connectedness says connected (2) but not in peer list (inconsistent state)
	if connectedness == 2 && isInPeerList {
		// Definitely connected, do nothing
		return
	}

	if connectedness == 1 {
		// Currently connecting, wait
		return
	}

	// Need to reconnect
	if connectedness == 2 && !isInPeerList {
		log.Debug().Str("peer", peerInfo.ID.String()).Msg("Connection state inconsistent, reconnecting...")
	} else {
		log.Info().Str("peer", peerInfo.ID.String()).Msg("Bootstrap node disconnected, reconnecting...")
	}

	ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
	defer cancel()

	if err := b.host.Connect(ctx, *peerInfo); err != nil {
		log.Debug().Err(err).Str("peer", peerInfo.ID.String()).Msg("Failed to reconnect to bootstrap node")
	} else {
		// Only log if we actually needed to reconnect
		if !isInPeerList {
			log.Info().Str("peer", peerInfo.ID.String()).Msg("Successfully reconnected to bootstrap node")
			// Announce ourselves after reconnecting
			go b.announceNodeInfo()
		}
	}
}

// discoveryMessage represents a message on the discovery topic
type discoveryMessage struct {
	Type     string              `json:"type"` // "announce" or "request"
	NodeInfo *CentrifugoNodeInfo `json:"node_info,omitempty"`
}

// setupDiscoveryTopic sets up the topic for exchanging Centrifugo node info
func (b *P2PBroker) setupDiscoveryTopic() error {
	topicName := b.config.TopicPrefix + ".discovery"
	topic, err := b.pubsub.Join(topicName)
	if err != nil {
		return fmt.Errorf("failed to join discovery topic: %w", err)
	}
	b.discoveryTopic = topic

	sub, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to discovery topic: %w", err)
	}
	b.discoverySub = sub

	// Handle incoming discovery messages
	go b.handleDiscoveryMessages()

	// Periodically announce our node info
	go b.announceLoop()

	log.Info().Str("topic", topicName).Msg("Discovery topic initialized")
	return nil
}

// handleDiscoveryMessages handles incoming discovery messages
func (b *P2PBroker) handleDiscoveryMessages() {
	for {
		msg, err := b.discoverySub.Next(b.ctx)
		if err != nil {
			if b.ctx.Err() != nil {
				return
			}
			continue
		}

		// Ignore messages from self
		if msg.ReceivedFrom == b.host.ID() {
			continue
		}

		var dm discoveryMessage
		if err := json.Unmarshal(msg.Data, &dm); err != nil {
			continue
		}

		switch dm.Type {
		case "announce":
			if dm.NodeInfo != nil {
				b.updateKnownNode(dm.NodeInfo)
			}
		case "request":
			// Someone is asking for node info, announce ourselves
			go b.announceNodeInfo()
		}
	}
}

// updateKnownNode updates or adds a known node
func (b *P2PBroker) updateKnownNode(info *CentrifugoNodeInfo) {
	if info == nil || info.PeerID == "" {
		return
	}

	// Don't store our own info
	if b.host != nil && info.PeerID == b.host.ID().String() {
		return
	}

	b.knownNodesMu.Lock()
	defer b.knownNodesMu.Unlock()

	existing, exists := b.knownNodes[info.PeerID]
	if !exists {
		b.knownNodes[info.PeerID] = info
		log.Info().
			Str("peer_id", info.PeerID).
			Str("ip", info.IP).
			Int("port", info.Port).
			Msg("Discovered new Centrifugo node")
	} else {
		// Update existing
		existing.IP = info.IP
		existing.Port = info.Port
		existing.TLS = info.TLS
		existing.WebSocket = info.WebSocket
		existing.LastSeen = info.LastSeen
	}
}

// announceLoop periodically announces our node info
func (b *P2PBroker) announceLoop() {
	// Initial announce after short delay
	time.Sleep(3 * time.Second)
	b.announceNodeInfo()

	// Then announce periodically
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.announceNodeInfo()
		}
	}
}

// announceNodeInfo announces our Centrifugo node info to the network
func (b *P2PBroker) announceNodeInfo() {
	if b.discoveryTopic == nil || b.host == nil {
		return
	}

	// Get our public IP
	ip := b.config.AdvertiseIP
	if ip == "" {
		ip = getPublicIP()
	}
	if ip == "" {
		// Fallback: try to get from host addresses
		for _, addr := range b.host.Addrs() {
			addrStr := addr.String()
			if strings.Contains(addrStr, "/ip4/") && !strings.Contains(addrStr, "127.0.0.1") {
				parts := strings.Split(addrStr, "/")
				for i, p := range parts {
					if p == "ip4" && i+1 < len(parts) {
						ip = parts[i+1]
						break
					}
				}
				if ip != "" {
					break
				}
			}
		}
	}

	if ip == "" {
		log.Warn().Msg("Cannot determine public IP for node announcement")
		return
	}

	port := b.config.CentrifugoPort
	if port == 0 {
		port = 8000 // Default Centrifugo port
	}

	scheme := "ws"
	if b.config.CentrifugoTLS {
		scheme = "wss"
	}

	nodeInfo := &CentrifugoNodeInfo{
		PeerID:    b.host.ID().String(),
		IP:        ip,
		Port:      port,
		TLS:       b.config.CentrifugoTLS,
		WebSocket: fmt.Sprintf("%s://%s:%d/connection/websocket", scheme, ip, port),
		LastSeen:  time.Now().Unix(),
	}

	dm := discoveryMessage{
		Type:     "announce",
		NodeInfo: nodeInfo,
	}

	data, err := json.Marshal(dm)
	if err != nil {
		return
	}

	if err := b.discoveryTopic.Publish(b.ctx, data); err != nil {
		log.Debug().Err(err).Msg("Failed to announce node info")
	}
}

// requestNodeInfo requests node info from all peers
func (b *P2PBroker) requestNodeInfo() {
	if b.discoveryTopic == nil {
		return
	}

	dm := discoveryMessage{
		Type: "request",
	}

	data, err := json.Marshal(dm)
	if err != nil {
		return
	}

	_ = b.discoveryTopic.Publish(b.ctx, data)
}

func (b *P2PBroker) connectToBootstrapNodes() {
	for _, addr := range b.config.BootstrapNodes {
		go b.connectToBootstrapNode(addr)
	}
}

func (b *P2PBroker) connectToBootstrapNode(addr string) {
	// 支持两种格式:
	// 1. 完整 multiaddr: /ip4/1.2.3.4/tcp/4001/p2p/QmXXX
	// 2. 简化格式: 1.2.3.4:4001 或 /ip4/1.2.3.4/tcp/4001

	var maddr multiaddr.Multiaddr
	var err error

	// 尝试解析为 multiaddr
	if strings.HasPrefix(addr, "/") {
		maddr, err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Error().Err(err).Str("addr", addr).Msg("Invalid multiaddr")
			return
		}
	} else {
		// 简化格式: IP:PORT
		parts := strings.Split(addr, ":")
		if len(parts) != 2 {
			log.Error().Str("addr", addr).Msg("Invalid address format, expected IP:PORT")
			return
		}
		maddr, err = multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", parts[0], parts[1]))
		if err != nil {
			log.Error().Err(err).Str("addr", addr).Msg("Failed to create multiaddr")
			return
		}
	}

	// 检查是否包含 peer ID
	peerInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err == nil {
		// 完整地址，直接连接
		if err := b.host.Connect(b.ctx, *peerInfo); err != nil {
			log.Warn().Err(err).Str("peer", peerInfo.ID.String()).Msg("Failed to connect to bootstrap node")
		} else {
			log.Info().Str("peer", peerInfo.ID.String()).Msg("Connected to bootstrap node")
		}
		return
	}

	// 不包含 peer ID，尝试通过 DHT 发现或定期重试
	log.Info().Str("addr", addr).Msg("Bootstrap address without peer ID, trying to discover...")

	// 定期尝试连接（等待对方节点启动并通告自己）
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for i := 0; i < 30; i++ { // 最多重试 30 次（5分钟）
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			// 尝试通过 DHT 查找连接到该地址的节点
			peers := b.host.Network().Peers()
			for _, p := range peers {
				addrs := b.host.Peerstore().Addrs(p)
				for _, a := range addrs {
					if strings.Contains(a.String(), maddr.String()) || addrMatches(a, maddr) {
						log.Info().Str("peer", p.String()).Msg("Found bootstrap peer via address match")
						return
					}
				}
			}

			// 如果还没发现，检查是否有其他节点连接进来了
			if len(peers) > 0 {
				log.Info().Int("peer_count", len(peers)).Msg("Have peers connected, discovery working")
				return
			}
		}
	}

	log.Warn().Str("addr", addr).Msg("Could not discover peer for bootstrap address")
}

// addrMatches 检查两个地址是否匹配（忽略 peer ID 部分）
func addrMatches(a, b multiaddr.Multiaddr) bool {
	// 提取 IP 和端口进行比较
	aStr := a.String()
	bStr := b.String()

	// 简单比较：检查 b 是否是 a 的前缀
	return strings.HasPrefix(aStr, bStr)
}

func (b *P2PBroker) discoverPeers() {
	routingDiscovery := drouting.NewRoutingDiscovery(b.dht)

	// Advertise ourselves
	_, err := routingDiscovery.Advertise(b.ctx, "centrifugo-network")
	if err != nil {
		log.Warn().Err(err).Msg("Failed to advertise")
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			peerChan, err := routingDiscovery.FindPeers(b.ctx, "centrifugo-network")
			if err != nil {
				continue
			}

			for p := range peerChan {
				if p.ID == b.host.ID() {
					continue
				}

				if b.host.Network().Connectedness(p.ID) != 2 { // NotConnected
					continue
				}

				if err := b.host.Connect(b.ctx, p); err != nil {
					log.Debug().Err(err).Str("peer", p.ID.String()).Msg("Failed to connect to discovered peer")
				} else {
					log.Info().Str("peer", p.ID.String()).Msg("Connected to discovered peer")
				}
			}
		}
	}
}

// RegisterBrokerEventHandler registers event handler for broker events
func (b *P2PBroker) RegisterBrokerEventHandler(h centrifuge.BrokerEventHandler) error {
	b.eventHandler = h

	if err := b.ensureStarted(); err != nil {
		return fmt.Errorf("failed to start P2P broker: %w", err)
	}

	log.Info().Str("broker", "p2p").Str("peer_id", b.host.ID().String()).Msg("P2P broker running")
	return nil
}

// RegisterControlEventHandler registers event handler for control events
func (b *P2PBroker) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	b.controlEventHandler = h

	// Ensure P2P network is initialized
	if err := b.ensureStarted(); err != nil {
		return fmt.Errorf("failed to start P2P broker: %w", err)
	}

	// Subscribe to control topic
	controlTopicName := b.config.TopicPrefix + ".control"
	topic, err := b.pubsub.Join(controlTopicName)
	if err != nil {
		return fmt.Errorf("failed to join control topic: %w", err)
	}
	b.controlTopic = topic

	sub, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to control topic: %w", err)
	}
	b.controlSub = sub

	go b.handleControlMessages()

	// Subscribe to node-specific topic
	nodeTopicName := b.config.TopicPrefix + ".node." + b.node.ID()
	nodeTopic, err := b.pubsub.Join(nodeTopicName)
	if err != nil {
		return fmt.Errorf("failed to join node topic: %w", err)
	}
	b.nodeTopic = nodeTopic

	nodeSub, err := nodeTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to node topic: %w", err)
	}
	b.nodeSub = nodeSub

	go b.handleNodeMessages()

	return nil
}

func (b *P2PBroker) handleControlMessages() {
	for {
		msg, err := b.controlSub.Next(b.ctx)
		if err != nil {
			log.Debug().Err(err).Msg("Control message handler exiting")
			return
		}

		// Ignore messages from self
		if msg.ReceivedFrom == b.host.ID() {
			continue
		}

		if b.controlEventHandler != nil {
			_ = b.controlEventHandler.HandleControl(msg.Data)
		}
	}
}

func (b *P2PBroker) handleNodeMessages() {
	for {
		msg, err := b.nodeSub.Next(b.ctx)
		if err != nil {
			log.Debug().Err(err).Msg("Node message handler exiting")
			return
		}

		if msg.ReceivedFrom == b.host.ID() {
			continue
		}

		if b.controlEventHandler != nil {
			_ = b.controlEventHandler.HandleControl(msg.Data)
		}
	}
}

// Close closes the P2P broker
func (b *P2PBroker) Close(_ context.Context) error {
	b.cancel()

	b.topicsMu.Lock()
	for _, tw := range b.topics {
		if tw.subscription != nil {
			tw.subscription.Cancel()
		}
		if tw.topic != nil {
			_ = tw.topic.Close()
		}
	}
	b.topicsMu.Unlock()

	// Close cached node topics
	b.nodeTopicsMu.Lock()
	for _, topic := range b.nodeTopics {
		if topic != nil {
			_ = topic.Close()
		}
	}
	b.nodeTopicsMu.Unlock()

	if b.controlSub != nil {
		b.controlSub.Cancel()
	}
	if b.controlTopic != nil {
		_ = b.controlTopic.Close()
	}
	if b.nodeSub != nil {
		b.nodeSub.Cancel()
	}
	if b.nodeTopic != nil {
		_ = b.nodeTopic.Close()
	}

	if b.dht != nil {
		_ = b.dht.Close()
	}

	if b.host != nil {
		_ = b.host.Close()
	}

	log.Info().Msg("P2P broker closed")
	return nil
}

// getOrCreateTopic safely gets an existing topic or creates a new one.
// This method is thread-safe and prevents duplicate Join calls.
func (b *P2PBroker) getOrCreateTopic(topicName, origChannel string) (*pubsub.Topic, error) {
	// Fast path: check with read lock
	b.topicsMu.RLock()
	if tw, exists := b.topics[topicName]; exists {
		topic := tw.topic
		b.topicsMu.RUnlock()
		return topic, nil
	}
	b.topicsMu.RUnlock()

	// Slow path: acquire write lock and double-check
	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()

	// Double-check after acquiring write lock
	if tw, exists := b.topics[topicName]; exists {
		return tw.topic, nil
	}

	// Join the topic
	topic, err := b.pubsub.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic: %w", err)
	}

	// Store for later
	b.topics[topicName] = &topicWrapper{topic: topic, origChannel: origChannel}
	return topic, nil
}

// Publish publishes a message to a channel
func (b *P2PBroker) Publish(ch string, data []byte, opts centrifuge.PublishOptions) (centrifuge.StreamPosition, bool, error) {
	startTotal := time.Now()

	pub := &centrifuge.Publication{
		Data: data,
		Info: opts.ClientInfo,
		Tags: opts.Tags,
		Time: time.Now().UnixMilli(),
	}

	push := &protocol.Push{
		Channel: ch,
		Pub: &protocol.Publication{
			Data:  data,
			Info:  infoToProto(opts.ClientInfo),
			Tags:  opts.Tags,
			Delta: opts.UseDelta,
			Time:  pub.Time,
		},
	}

	byteMessage, err := push.MarshalVT()
	if err != nil {
		return centrifuge.StreamPosition{}, false, err
	}

	topicName := b.clientTopicName(ch)

	startGetTopic := time.Now()
	topic, err := b.getOrCreateTopic(topicName, ch)
	getTopicDuration := time.Since(startGetTopic)
	if err != nil {
		return centrifuge.StreamPosition{}, false, fmt.Errorf("failed to get topic: %w", err)
	}

	startP2PPublish := time.Now()
	if err := topic.Publish(b.ctx, byteMessage); err != nil {
		return centrifuge.StreamPosition{}, false, fmt.Errorf("failed to publish: %w", err)
	}
	p2pPublishDuration := time.Since(startP2PPublish)

	// Get stats for logging
	b.topicsMu.RLock()
	topicsCount := len(b.topics)
	b.topicsMu.RUnlock()
	b.nodeTopicsMu.RLock()
	nodeTopicsCount := len(b.nodeTopics)
	b.nodeTopicsMu.RUnlock()

	log.Debug().
		Str("channel", ch).
		Int("data_len", len(data)).
		Dur("get_topic_ms", getTopicDuration).
		Dur("p2p_publish_ms", p2pPublishDuration).
		Int("topics_count", topicsCount).
		Int("node_topics_count", nodeTopicsCount).
		Msg("P2P message published")

	// Distribute to local subscribers via event handler
	// This is required because centrifuge node relies on broker to call HandlePublication
	if b.eventHandler != nil {
		startHandler := time.Now()
		err := b.eventHandler.HandlePublication(ch, pub, centrifuge.StreamPosition{}, opts.UseDelta, nil)
		handlerDuration := time.Since(startHandler)
		totalDuration := time.Since(startTotal)

		// Log warning if slow
		if totalDuration > 100*time.Millisecond {
			log.Warn().
				Str("channel", ch).
				Dur("total_ms", totalDuration).
				Dur("get_topic_ms", getTopicDuration).
				Dur("p2p_publish_ms", p2pPublishDuration).
				Dur("handler_ms", handlerDuration).
				Int("topics_count", topicsCount).
				Msg("Slow publish detected")
		}

		return centrifuge.StreamPosition{}, false, err
	}

	return centrifuge.StreamPosition{}, false, nil
}

// PublishJoin publishes join event
func (b *P2PBroker) PublishJoin(ch string, info *centrifuge.ClientInfo) error {
	push := &protocol.Push{
		Channel: ch,
		Join: &protocol.Join{
			Info: infoToProto(info),
		},
	}

	byteMessage, err := push.MarshalVT()
	if err != nil {
		return err
	}

	topicName := b.clientTopicName(ch)

	b.topicsMu.RLock()
	tw, exists := b.topics[topicName]
	b.topicsMu.RUnlock()

	if exists {
		_ = tw.topic.Publish(b.ctx, byteMessage)
	}

	// Distribute to local subscribers via event handler
	if b.eventHandler != nil {
		return b.eventHandler.HandleJoin(ch, info)
	}
	return nil
}

// PublishLeave publishes leave event
func (b *P2PBroker) PublishLeave(ch string, info *centrifuge.ClientInfo) error {
	push := &protocol.Push{
		Channel: ch,
		Leave: &protocol.Leave{
			Info: infoToProto(info),
		},
	}

	byteMessage, err := push.MarshalVT()
	if err != nil {
		return err
	}

	topicName := b.clientTopicName(ch)

	b.topicsMu.RLock()
	tw, exists := b.topics[topicName]
	b.topicsMu.RUnlock()

	if exists {
		_ = tw.topic.Publish(b.ctx, byteMessage)
	}

	// Distribute to local subscribers via event handler
	if b.eventHandler != nil {
		return b.eventHandler.HandleLeave(ch, info)
	}
	return nil
}

// PublishControl publishes control message
func (b *P2PBroker) PublishControl(data []byte, nodeID, _ string) error {
	if nodeID == "" {
		// Broadcast to all nodes
		if b.controlTopic == nil {
			return nil
		}
		return b.controlTopic.Publish(b.ctx, data)
	}

	// Send to specific node
	topic, err := b.getOrCreateNodeTopic(nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node topic: %w", err)
	}

	return topic.Publish(b.ctx, data)
}

// getOrCreateNodeTopic safely gets an existing node topic or creates a new one.
func (b *P2PBroker) getOrCreateNodeTopic(nodeID string) (*pubsub.Topic, error) {
	nodeTopicName := b.config.TopicPrefix + ".node." + nodeID

	// Fast path: check with read lock
	b.nodeTopicsMu.RLock()
	if topic, exists := b.nodeTopics[nodeTopicName]; exists {
		b.nodeTopicsMu.RUnlock()
		return topic, nil
	}
	b.nodeTopicsMu.RUnlock()

	// Slow path: acquire write lock and double-check
	b.nodeTopicsMu.Lock()
	defer b.nodeTopicsMu.Unlock()

	// Double-check after acquiring write lock
	if topic, exists := b.nodeTopics[nodeTopicName]; exists {
		return topic, nil
	}

	// Join the topic
	topic, err := b.pubsub.Join(nodeTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join node topic: %w", err)
	}

	b.nodeTopics[nodeTopicName] = topic
	return topic, nil
}

// Subscribe subscribes to a channel
func (b *P2PBroker) Subscribe(ch string) error {
	topicName := b.clientTopicName(ch)

	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()

	// Check if already subscribed
	if tw, exists := b.topics[topicName]; exists && tw.subscription != nil {
		return nil
	}

	// Check if topic exists but not subscribed yet (e.g., created by Publish)
	var topic *pubsub.Topic
	var err error
	if tw, exists := b.topics[topicName]; exists {
		// Reuse existing topic
		topic = tw.topic
	} else {
		// Join new topic
		topic, err = b.pubsub.Join(topicName)
		if err != nil {
			return fmt.Errorf("failed to join topic: %w", err)
		}
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	tw := &topicWrapper{
		topic:        topic,
		subscription: sub,
		origChannel:  ch,
	}
	b.topics[topicName] = tw

	// Start message handler goroutine
	go b.handleChannelMessages(tw)

	log.Debug().Str("channel", ch).Msg("Subscribed to channel")
	return nil
}

// Unsubscribe unsubscribes from a channel
func (b *P2PBroker) Unsubscribe(ch string) error {
	topicName := b.clientTopicName(ch)

	b.topicsMu.Lock()
	defer b.topicsMu.Unlock()

	tw, exists := b.topics[topicName]
	if !exists {
		return nil
	}

	if tw.subscription != nil {
		tw.subscription.Cancel()
	}
	if tw.topic != nil {
		_ = tw.topic.Close()
	}

	delete(b.topics, topicName)
	log.Debug().Str("channel", ch).Msg("Unsubscribed from channel")
	return nil
}

// History is not supported in P2P broker (no persistence)
func (b *P2PBroker) History(_ string, _ centrifuge.HistoryOptions) ([]*centrifuge.Publication, centrifuge.StreamPosition, error) {
	return nil, centrifuge.StreamPosition{}, centrifuge.ErrorNotAvailable
}

// RemoveHistory is not supported in P2P broker
func (b *P2PBroker) RemoveHistory(_ string) error {
	return centrifuge.ErrorNotAvailable
}

func (b *P2PBroker) handleChannelMessages(tw *topicWrapper) {
	for {
		msg, err := tw.subscription.Next(b.ctx)
		if err != nil {
			// Exit if global context is cancelled or subscription is cancelled
			if b.ctx.Err() != nil {
				log.Debug().Str("channel", tw.origChannel).Msg("Channel message handler exiting: context cancelled")
				return
			}
			// Also exit if subscription returns error (likely cancelled)
			log.Debug().Str("channel", tw.origChannel).Err(err).Msg("Channel message handler exiting: subscription error")
			return
		}

		// Ignore messages from self (local messages are handled by centrifuge node directly)
		if msg.ReceivedFrom == b.host.ID() {
			log.Debug().
				Str("channel", tw.origChannel).
				Str("from", msg.ReceivedFrom.String()).
				Msg("P2P ignoring message from self (local delivery handled by node)")
			continue
		}

		log.Debug().
			Str("channel", tw.origChannel).
			Str("from", msg.ReceivedFrom.String()).
			Int("data_len", len(msg.Data)).
			Msg("P2P received message from remote peer")

		var push protocol.Push
		if err := push.UnmarshalVT(msg.Data); err != nil {
			log.Warn().Err(err).Msg("Failed to unmarshal push message")
			continue
		}

		if push.Pub != nil {
			if b.eventHandler != nil {
				log.Debug().
					Str("channel", tw.origChannel).
					Msg("P2P forwarding publication to event handler")
				_ = b.eventHandler.HandlePublication(
					tw.origChannel,
					pubFromProto(push.Pub, ""),
					centrifuge.StreamPosition{},
					push.Pub.Delta,
					nil,
				)
			}
		} else if push.Join != nil {
			if b.eventHandler != nil {
				_ = b.eventHandler.HandleJoin(push.Channel, infoFromProto(push.Join.Info))
			}
		} else if push.Leave != nil {
			if b.eventHandler != nil {
				_ = b.eventHandler.HandleLeave(push.Channel, infoFromProto(push.Leave.Info))
			}
		}
	}
}

func (b *P2PBroker) clientTopicName(ch string) string {
	return b.config.TopicPrefix + ".client." + ch
}

// GetPeerID returns the peer ID of this node
func (b *P2PBroker) GetPeerID() string {
	if b.host == nil {
		return ""
	}
	return b.host.ID().String()
}

// GetPeerCount returns the number of connected peers
func (b *P2PBroker) GetPeerCount() int {
	if b.host == nil {
		return 0
	}
	return len(b.host.Network().Peers())
}

// GetPeers returns list of connected peer IDs
func (b *P2PBroker) GetPeers() []string {
	if b.host == nil {
		return nil
	}
	peers := b.host.Network().Peers()
	result := make([]string, len(peers))
	for i, p := range peers {
		result[i] = p.String()
	}
	return result
}

// PeerInfo represents detailed information about a connected peer
type PeerInfo struct {
	PeerID      string   `json:"peer_id"`
	Addresses   []string `json:"addresses"`
	Latency     int64    `json:"latency_ms"` // Latency in milliseconds, -1 if unknown
	Connected   bool     `json:"connected"`
	Protocols   []string `json:"protocols"`
	ConnectedAt string   `json:"connected_at,omitempty"`
}

// GetPeersDetailed returns detailed information about all connected peers
func (b *P2PBroker) GetPeersDetailed() []PeerInfo {
	if b.host == nil {
		return nil
	}

	peers := b.host.Network().Peers()
	result := make([]PeerInfo, 0, len(peers))

	for _, p := range peers {
		info := PeerInfo{
			PeerID:    p.String(),
			Connected: b.host.Network().Connectedness(p) == 2, // Connected
			Latency:   -1,
		}

		// Get peer addresses
		addrs := b.host.Peerstore().Addrs(p)
		info.Addresses = make([]string, len(addrs))
		for i, addr := range addrs {
			info.Addresses[i] = addr.String()
		}

		// Get peer protocols
		protocols, err := b.host.Peerstore().GetProtocols(p)
		if err == nil {
			info.Protocols = make([]string, len(protocols))
			for i, proto := range protocols {
				info.Protocols[i] = string(proto)
			}
		}

		// Get latency from peerstore
		latency := b.host.Peerstore().LatencyEWMA(p)
		if latency > 0 {
			info.Latency = latency.Milliseconds()
		}

		result = append(result, info)
	}

	return result
}

// PingResult represents the result of a ping operation
type PingResult struct {
	PeerID    string `json:"peer_id"`
	LatencyMs int64  `json:"latency_ms"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
}

// PingPeer pings a specific peer and returns the latency
func (b *P2PBroker) PingPeer(peerIDStr string) PingResult {
	result := PingResult{
		PeerID:  peerIDStr,
		Success: false,
	}

	if b.host == nil {
		result.Error = "P2P host not initialized"
		return result
	}

	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		result.Error = fmt.Sprintf("invalid peer ID: %v", err)
		return result
	}

	// Check if connected
	if b.host.Network().Connectedness(peerID) != 2 {
		result.Error = "peer not connected"
		return result
	}

	// Use a simple message round-trip to measure latency
	start := time.Now()

	// Try to open a stream to measure latency
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()

	stream, err := b.host.NewStream(ctx, peerID, "/ipfs/ping/1.0.0")
	if err != nil {
		// Fallback: use stored latency
		latency := b.host.Peerstore().LatencyEWMA(peerID)
		if latency > 0 {
			result.LatencyMs = latency.Milliseconds()
			result.Success = true
			return result
		}
		result.Error = fmt.Sprintf("failed to ping: %v", err)
		return result
	}
	defer stream.Close()

	// Send ping data
	pingData := make([]byte, 32)
	rand.Read(pingData)

	_, err = stream.Write(pingData)
	if err != nil {
		result.Error = fmt.Sprintf("failed to send ping: %v", err)
		return result
	}

	// Read pong response
	pongData := make([]byte, 32)
	_, err = io.ReadFull(stream, pongData)
	if err != nil {
		result.Error = fmt.Sprintf("failed to read pong: %v", err)
		return result
	}

	latency := time.Since(start)
	result.LatencyMs = latency.Milliseconds()
	result.Success = true

	// Record latency in peerstore
	b.host.Peerstore().RecordLatency(peerID, latency)

	return result
}

// PingAllPeers pings all connected peers and returns their latencies
func (b *P2PBroker) PingAllPeers() []PingResult {
	if b.host == nil {
		return nil
	}

	peers := b.host.Network().Peers()
	results := make([]PingResult, 0, len(peers))

	for _, p := range peers {
		result := b.PingPeer(p.String())
		results = append(results, result)
	}

	return results
}

// NodeStatus represents the full status of this P2P node
type NodeStatus struct {
	Enabled        bool       `json:"enabled"`
	PeerID         string     `json:"peer_id"`
	ListenPort     int        `json:"listen_port"`
	Addresses      []string   `json:"addresses"`
	PeerCount      int        `json:"peer_count"`
	Peers          []PeerInfo `json:"peers"`
	BootstrapNodes []string   `json:"bootstrap_nodes"`
	EnableMDNS     bool       `json:"enable_mdns"`
	TopicPrefix    string     `json:"topic_prefix"`
}

// GetNodeStatus returns comprehensive status of this P2P node
func (b *P2PBroker) GetNodeStatus() NodeStatus {
	status := NodeStatus{
		Enabled:        b.config.Enabled,
		ListenPort:     b.config.ListenPort,
		BootstrapNodes: b.config.BootstrapNodes,
		EnableMDNS:     b.config.EnableMDNS,
		TopicPrefix:    b.config.TopicPrefix,
	}

	if b.host != nil {
		status.PeerID = b.host.ID().String()
		status.PeerCount = len(b.host.Network().Peers())
		status.Peers = b.GetPeersDetailed()

		// Get all listen addresses
		addrs := b.host.Addrs()
		status.Addresses = make([]string, len(addrs))
		for i, addr := range addrs {
			status.Addresses[i] = addr.String()
		}
	}

	return status
}

// GetStatus returns broker status (legacy, kept for compatibility)
func (b *P2PBroker) GetStatus() map[string]interface{} {
	b.topicsMu.RLock()
	topicsCount := len(b.topics)
	topicsList := make([]string, 0, topicsCount)
	for name := range b.topics {
		topicsList = append(topicsList, name)
	}
	b.topicsMu.RUnlock()

	b.nodeTopicsMu.RLock()
	nodeTopicsCount := len(b.nodeTopics)
	b.nodeTopicsMu.RUnlock()

	b.knownNodesMu.RLock()
	knownNodesCount := len(b.knownNodes)
	b.knownNodesMu.RUnlock()

	return map[string]interface{}{
		"enabled":           b.config.Enabled,
		"peer_id":           b.GetPeerID(),
		"peer_count":        b.GetPeerCount(),
		"peers":             b.GetPeers(),
		"listen_port":       b.config.ListenPort,
		"topics_count":      topicsCount,
		"node_topics_count": nodeTopicsCount,
		"known_nodes_count": knownNodesCount,
		"topics":            topicsList,
	}
}

// GetMultiaddrs returns the full multiaddresses of this node (including peer ID)
func (b *P2PBroker) GetMultiaddrs() []string {
	if b.host == nil {
		return nil
	}

	addrs := b.host.Addrs()
	peerID := b.host.ID()
	result := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		fullAddr := fmt.Sprintf("%s/p2p/%s", addr.String(), peerID.String())
		result = append(result, fullAddr)
	}

	return result
}

// GetCentrifugoNodes returns all known Centrifugo nodes (including self)
// This is what frontend clients should use to find available nodes
func (b *P2PBroker) GetCentrifugoNodes() []CentrifugoNodeInfo {
	result := make([]CentrifugoNodeInfo, 0)
	addedPeers := make(map[string]bool)

	// Add self first
	selfInfo := b.GetSelfNodeInfo()
	if selfInfo != nil {
		result = append(result, *selfInfo)
		addedPeers[selfInfo.PeerID] = true
	}

	// First, add nodes from discovery (they have complete info)
	b.knownNodesMu.RLock()
	for _, info := range b.knownNodes {
		if addedPeers[info.PeerID] {
			continue
		}
		// Check if node is still connected
		if b.host != nil {
			peerID, err := peer.Decode(info.PeerID)
			if err == nil && b.host.Network().Connectedness(peerID) == 2 {
				result = append(result, *info)
				addedPeers[info.PeerID] = true
			}
		}
	}
	b.knownNodesMu.RUnlock()

	// Also add connected peers that we haven't received discovery info for
	// Use their IP from the P2P connection
	if b.host != nil {
		for _, p := range b.host.Network().Peers() {
			peerIDStr := p.String()
			if addedPeers[peerIDStr] {
				continue
			}

			// Try to get IP from peer's addresses
			addrs := b.host.Peerstore().Addrs(p)
			var peerIP string
			for _, addr := range addrs {
				addrStr := addr.String()
				// Extract IP from multiaddr like /ip4/1.2.3.4/tcp/4001
				if strings.Contains(addrStr, "/ip4/") && !strings.Contains(addrStr, "127.0.0.1") {
					parts := strings.Split(addrStr, "/")
					for i, part := range parts {
						if part == "ip4" && i+1 < len(parts) {
							peerIP = parts[i+1]
							break
						}
					}
				}
				if peerIP != "" {
					break
				}
			}

			if peerIP != "" {
				// Assume same Centrifugo port as us (reasonable default)
				port := b.config.CentrifugoPort
				if port == 0 {
					port = 8000
				}
				scheme := "ws"
				if b.config.CentrifugoTLS {
					scheme = "wss"
				}

				result = append(result, CentrifugoNodeInfo{
					PeerID:    peerIDStr,
					IP:        peerIP,
					Port:      port,
					TLS:       b.config.CentrifugoTLS,
					WebSocket: fmt.Sprintf("%s://%s:%d/connection/websocket", scheme, peerIP, port),
					LastSeen:  time.Now().Unix(),
				})
				addedPeers[peerIDStr] = true
			}
		}
	}

	return result
}

// GetSelfNodeInfo returns this node's Centrifugo connection info
func (b *P2PBroker) GetSelfNodeInfo() *CentrifugoNodeInfo {
	if b.host == nil {
		return nil
	}

	ip := b.config.AdvertiseIP
	if ip == "" {
		ip = getPublicIP()
	}
	if ip == "" {
		return nil
	}

	port := b.config.CentrifugoPort
	if port == 0 {
		port = 8000
	}

	scheme := "ws"
	if b.config.CentrifugoTLS {
		scheme = "wss"
	}

	return &CentrifugoNodeInfo{
		PeerID:    b.host.ID().String(),
		IP:        ip,
		Port:      port,
		TLS:       b.config.CentrifugoTLS,
		WebSocket: fmt.Sprintf("%s://%s:%d/connection/websocket", scheme, ip, port),
		LastSeen:  time.Now().Unix(),
	}
}

// RefreshNodes requests fresh node info from all peers
func (b *P2PBroker) RefreshNodes() {
	b.requestNodeInfo()
}

// CleanupOfflineNodes removes nodes that are no longer connected
func (b *P2PBroker) CleanupOfflineNodes() int {
	if b.host == nil {
		return 0
	}

	b.knownNodesMu.Lock()
	defer b.knownNodesMu.Unlock()

	removed := 0
	for peerIDStr, _ := range b.knownNodes {
		peerID, err := peer.Decode(peerIDStr)
		if err != nil {
			delete(b.knownNodes, peerIDStr)
			removed++
			continue
		}

		if b.host.Network().Connectedness(peerID) != 2 {
			delete(b.knownNodes, peerIDStr)
			removed++
		}
	}

	return removed
}

// mdnsNotifee handles mDNS discovery events
type mdnsNotifee struct {
	broker *P2PBroker
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.broker.host.ID() {
		return
	}

	if err := n.broker.host.Connect(n.broker.ctx, pi); err != nil {
		log.Debug().Err(err).Str("peer", pi.ID.String()).Msg("Failed to connect to mDNS peer")
	} else {
		log.Info().Str("peer", pi.ID.String()).Msg("Connected to mDNS peer")
	}
}

// Helper functions

func listenAddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}

func getPublicIP() string {
	client := &http.Client{Timeout: 5 * time.Second}
	apis := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}

	for _, api := range apis {
		resp, err := client.Get(api)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		if ip != "" {
			return ip
		}
	}
	return ""
}

func infoFromProto(v *protocol.ClientInfo) *centrifuge.ClientInfo {
	if v == nil {
		return nil
	}
	info := &centrifuge.ClientInfo{
		ClientID: v.GetClient(),
		UserID:   v.GetUser(),
	}
	if len(v.ConnInfo) > 0 {
		info.ConnInfo = v.ConnInfo
	}
	if len(v.ChanInfo) > 0 {
		info.ChanInfo = v.ChanInfo
	}
	return info
}

func infoToProto(v *centrifuge.ClientInfo) *protocol.ClientInfo {
	if v == nil {
		return nil
	}
	info := &protocol.ClientInfo{
		Client: v.ClientID,
		User:   v.UserID,
	}
	if len(v.ConnInfo) > 0 {
		info.ConnInfo = v.ConnInfo
	}
	if len(v.ChanInfo) > 0 {
		info.ChanInfo = v.ChanInfo
	}
	return info
}

func pubFromProto(pub *protocol.Publication, specificChannel string) *centrifuge.Publication {
	if pub == nil {
		return nil
	}
	return &centrifuge.Publication{
		Offset:  pub.GetOffset(),
		Data:    pub.Data,
		Info:    infoFromProto(pub.GetInfo()),
		Channel: specificChannel,
		Tags:    pub.GetTags(),
		Time:    pub.Time,
	}
}

// MarshalJSON for Config to support JSON serialization
func (c Config) MarshalJSON() ([]byte, error) {
	type Alias Config
	return json.Marshal(&struct{ Alias }{Alias: Alias(c)})
}
