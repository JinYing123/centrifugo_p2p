package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// 消息类型
const (
	MsgPing     = "PING"     // 心跳探测
	MsgPong     = "PONG"     // 心跳响应
	MsgRegister = "REGISTER" // 注册到引导节点
	MsgNodes    = "NODES"    // 返回已知节点列表
	MsgDiscover = "DISCOVER" // 请求发现节点
)

// Message P2P消息格式
type Message struct {
	Type  string      `json:"type"`
	From  *NodeInfo   `json:"from"`
	Nodes []*NodeInfo `json:"nodes,omitempty"`
	Time  int64       `json:"time"`
}

// NodeInfo 节点信息
type NodeInfo struct {
	ID       string    `json:"id"`
	IP       string    `json:"ip"`
	Port     int       `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	Online   bool      `json:"online"`
}

// Config P2P配置
type Config struct {
	Enabled        bool          `json:"enabled"`
	DiscoveryPort  int           `json:"discovery_port"`
	AdvertiseIP    string        `json:"advertise_ip"`
	AdvertisePort  int           `json:"advertise_port"`
	Interval       time.Duration `json:"interval"`
	BootstrapNodes []string      `json:"bootstrap_nodes"`
}

// DiscoveryService P2P节点发现服务
type DiscoveryService struct {
	config   *Config
	nodeInfo *NodeInfo

	mu         sync.RWMutex
	knownNodes map[string]*NodeInfo
	conn       *net.UDPConn

	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	logger  zerolog.Logger
}

// NewDiscoveryService 创建发现服务
func NewDiscoveryService(cfg *Config, centrifugoPort int) *DiscoveryService {
	if cfg.AdvertisePort == 0 {
		cfg.AdvertisePort = centrifugoPort
	}

	nodeID := fmt.Sprintf("node-%s", uuid.New().String()[:8])

	return &DiscoveryService{
		config: cfg,
		nodeInfo: &NodeInfo{
			ID:     nodeID,
			IP:     cfg.AdvertiseIP,
			Port:   cfg.AdvertisePort,
			Online: true,
		},
		knownNodes: make(map[string]*NodeInfo),
		logger:     zerolog.Nop(),
	}
}

// SetLogger 设置日志器
func (ds *DiscoveryService) SetLogger(logger zerolog.Logger) {
	ds.logger = logger
}

// Run 实现service.Service接口
func (ds *DiscoveryService) Run(ctx context.Context) error {
	if !ds.config.Enabled {
		return nil
	}

	ds.ctx, ds.cancel = context.WithCancel(ctx)
	ds.started = true

	// 如果未配置公网IP，尝试获取
	if ds.config.AdvertiseIP == "" {
		ds.logger.Info().Msg("未配置公网IP，正在自动获取...")
		ip := getPublicIP()
		if ip != "" {
			ds.config.AdvertiseIP = ip
			ds.nodeInfo.IP = ip
			ds.logger.Info().Str("public_ip", ip).Msg("成功获取公网IP")
		} else {
			ds.logger.Warn().Msg("无法获取公网IP，P2P功能可能受限")
		}
	}

	ds.logger.Info().
		Str("node_id", ds.nodeInfo.ID).
		Int("discovery_port", ds.config.DiscoveryPort).
		Str("advertise_addr", fmt.Sprintf("%s:%d", ds.nodeInfo.IP, ds.nodeInfo.Port)).
		Int("bootstrap_nodes_count", len(ds.config.BootstrapNodes)).
		Msg("P2P节点发现服务启动")

	// 启动UDP监听
	if err := ds.startUDP(); err != nil {
		return fmt.Errorf("启动UDP失败: %v", err)
	}

	// 启动接收协程
	go ds.receiveLoop()

	// 连接引导节点
	go ds.connectBootstrapNodes()

	// 启动心跳协程
	go ds.heartbeatLoop()

	// 启动清理协程
	go ds.cleanupLoop()

	// 等待退出信号
	<-ds.ctx.Done()

	// 清理资源
	if ds.conn != nil {
		ds.conn.Close()
	}

	ds.logger.Info().Msg("P2P节点发现服务已停止")
	return nil
}

// startUDP 启动UDP监听
func (ds *DiscoveryService) startUDP() error {
	addr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: ds.config.DiscoveryPort,
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("监听UDP端口 %d 失败: %v", ds.config.DiscoveryPort, err)
	}

	ds.conn = conn
	ds.logger.Info().Int("port", ds.config.DiscoveryPort).Msg("UDP监听已启动")
	return nil
}

// connectBootstrapNodes 连接引导节点
func (ds *DiscoveryService) connectBootstrapNodes() {
	if len(ds.config.BootstrapNodes) == 0 {
		ds.logger.Info().Msg("未配置引导节点，等待其他节点连接...")
		return
	}

	ds.logger.Info().
		Strs("bootstrap_nodes", ds.config.BootstrapNodes).
		Msg("正在连接引导节点...")

	for _, addr := range ds.config.BootstrapNodes {
		go ds.connectToNode(addr)
	}
}

// connectToNode 连接到指定节点
func (ds *DiscoveryService) connectToNode(addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		ds.logger.Error().Err(err).Str("addr", addr).Msg("解析节点地址失败")
		return
	}

	// 发送注册消息
	ds.sendMessage(udpAddr, &Message{
		Type: MsgRegister,
		From: ds.nodeInfo,
		Time: time.Now().Unix(),
	})

	ds.logger.Info().Str("addr", addr).Msg("已向引导节点发送注册请求")
}

// sendMessage 发送消息到指定地址
func (ds *DiscoveryService) sendMessage(addr *net.UDPAddr, msg *Message) {
	if ds.conn == nil {
		return
	}

	msg.From = ds.nodeInfo
	msg.Time = time.Now().Unix()

	data, err := json.Marshal(msg)
	if err != nil {
		ds.logger.Error().Err(err).Msg("序列化消息失败")
		return
	}

	if _, err := ds.conn.WriteToUDP(data, addr); err != nil {
		ds.logger.Debug().Err(err).Str("addr", addr.String()).Msg("发送消息失败")
	}
}

// receiveLoop 接收消息循环
func (ds *DiscoveryService) receiveLoop() {
	buffer := make([]byte, 65535)

	for {
		select {
		case <-ds.ctx.Done():
			return
		default:
			if ds.conn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			ds.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, addr, err := ds.conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				continue
			}

			var msg Message
			if err := json.Unmarshal(buffer[:n], &msg); err != nil {
				continue
			}

			ds.handleMessage(addr, &msg)
		}
	}
}

// handleMessage 处理收到的消息
func (ds *DiscoveryService) handleMessage(addr *net.UDPAddr, msg *Message) {
	if msg.From == nil || msg.From.ID == ds.nodeInfo.ID {
		return
	}

	switch msg.Type {
	case MsgPing:
		ds.handlePing(addr, msg)
	case MsgPong:
		ds.handlePong(addr, msg)
	case MsgRegister:
		ds.handleRegister(addr, msg)
	case MsgDiscover:
		ds.handleDiscover(addr, msg)
	case MsgNodes:
		ds.handleNodes(addr, msg)
	}
}

// handlePing 处理心跳请求
func (ds *DiscoveryService) handlePing(addr *net.UDPAddr, msg *Message) {
	// 更新节点信息
	ds.updateNode(msg.From)

	// 回复PONG
	ds.sendMessage(addr, &Message{Type: MsgPong})
}

// handlePong 处理心跳响应
func (ds *DiscoveryService) handlePong(addr *net.UDPAddr, msg *Message) {
	ds.updateNode(msg.From)
}

// handleRegister 处理注册请求
func (ds *DiscoveryService) handleRegister(addr *net.UDPAddr, msg *Message) {
	ds.logger.Info().
		Str("node_id", msg.From.ID).
		Str("addr", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port)).
		Msg("收到节点注册请求")

	// 更新节点信息
	ds.updateNode(msg.From)

	// 返回已知节点列表
	nodes := ds.GetKnownNodes()
	ds.sendMessage(addr, &Message{
		Type:  MsgNodes,
		Nodes: nodesToInfoList(nodes),
	})
}

// handleDiscover 处理发现请求
func (ds *DiscoveryService) handleDiscover(addr *net.UDPAddr, msg *Message) {
	ds.updateNode(msg.From)

	nodes := ds.GetKnownNodes()
	ds.sendMessage(addr, &Message{
		Type:  MsgNodes,
		Nodes: nodesToInfoList(nodes),
	})
}

// handleNodes 处理节点列表响应
func (ds *DiscoveryService) handleNodes(addr *net.UDPAddr, msg *Message) {
	ds.updateNode(msg.From)

	if len(msg.Nodes) == 0 {
		return
	}

	ds.logger.Info().
		Int("nodes_count", len(msg.Nodes)).
		Msg("收到节点列表")

	// 添加新节点并尝试连接
	for _, node := range msg.Nodes {
		if node.ID == ds.nodeInfo.ID {
			continue
		}

		ds.mu.RLock()
		_, exists := ds.knownNodes[node.ID]
		ds.mu.RUnlock()

		if !exists {
			// 尝试连接新节点
			nodeAddr := fmt.Sprintf("%s:%d", node.IP, ds.config.DiscoveryPort)
			go ds.connectToNode(nodeAddr)
		}
	}
}

// updateNode 更新节点信息
func (ds *DiscoveryService) updateNode(node *NodeInfo) {
	if node == nil || node.ID == ds.nodeInfo.ID {
		return
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, exists := ds.knownNodes[node.ID]
	if !exists {
		node.LastSeen = time.Now()
		node.Online = true
		ds.knownNodes[node.ID] = node
		ds.logger.Info().
			Str("node_id", node.ID).
			Str("addr", fmt.Sprintf("%s:%d", node.IP, node.Port)).
			Msg("发现新节点")
	} else {
		existing.LastSeen = time.Now()
		existing.Online = true
		existing.IP = node.IP
		existing.Port = node.Port
	}
}

// heartbeatLoop 心跳循环
func (ds *DiscoveryService) heartbeatLoop() {
	ticker := time.NewTicker(ds.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ds.ctx.Done():
			return
		case <-ticker.C:
			ds.sendHeartbeats()
		}
	}
}

// sendHeartbeats 向所有已知节点发送心跳
func (ds *DiscoveryService) sendHeartbeats() {
	ds.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(ds.knownNodes))
	for _, node := range ds.knownNodes {
		if node.Online {
			nodes = append(nodes, node)
		}
	}
	ds.mu.RUnlock()

	for _, node := range nodes {
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.IP, ds.config.DiscoveryPort))
		if err != nil {
			continue
		}
		ds.sendMessage(addr, &Message{Type: MsgPing})
	}

	// 也向引导节点发送心跳，保持连接
	for _, addr := range ds.config.BootstrapNodes {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}
		ds.sendMessage(udpAddr, &Message{Type: MsgPing})
	}
}

// cleanupLoop 清理离线节点
func (ds *DiscoveryService) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ds.ctx.Done():
			return
		case <-ticker.C:
			ds.mu.Lock()
			now := time.Now()
			for id, node := range ds.knownNodes {
				if now.Sub(node.LastSeen) > 3*ds.config.Interval {
					if node.Online {
						node.Online = false
						ds.logger.Info().
							Str("node_id", id).
							Msg("节点离线")
					}
				}
				// 超过10分钟未响应，删除节点
				if now.Sub(node.LastSeen) > 10*time.Minute {
					delete(ds.knownNodes, id)
					ds.logger.Info().
						Str("node_id", id).
						Msg("移除超时节点")
				}
			}
			ds.mu.Unlock()
		}
	}
}

// GetKnownNodes 获取已知在线节点
func (ds *DiscoveryService) GetKnownNodes() []NodeInfo {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(ds.knownNodes))
	for _, node := range ds.knownNodes {
		if node.Online {
			nodes = append(nodes, *node)
		}
	}
	return nodes
}

// GetAllNodes 获取所有节点（包括离线）
func (ds *DiscoveryService) GetAllNodes() []NodeInfo {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(ds.knownNodes))
	for _, node := range ds.knownNodes {
		nodes = append(nodes, *node)
	}
	return nodes
}

// GetNodeID 获取节点ID
func (ds *DiscoveryService) GetNodeID() string {
	return ds.nodeInfo.ID
}

// GetNodeInfo 获取本节点信息
func (ds *DiscoveryService) GetNodeInfo() *NodeInfo {
	return ds.nodeInfo
}

// GetStatus 获取服务状态
func (ds *DiscoveryService) GetStatus() map[string]interface{} {
	nodes := ds.GetKnownNodes()
	allNodes := ds.GetAllNodes()
	return map[string]interface{}{
		"node_id":         ds.nodeInfo.ID,
		"enabled":         ds.config.Enabled,
		"discovery_port":  ds.config.DiscoveryPort,
		"advertise_addr":  fmt.Sprintf("%s:%d", ds.nodeInfo.IP, ds.nodeInfo.Port),
		"bootstrap_nodes": ds.config.BootstrapNodes,
		"online_nodes":    len(nodes),
		"total_nodes":     len(allNodes),
		"known_nodes":     nodes,
	}
}

// nodesToInfoList 转换节点列表
func nodesToInfoList(nodes []NodeInfo) []*NodeInfo {
	result := make([]*NodeInfo, len(nodes))
	for i := range nodes {
		result[i] = &nodes[i]
	}
	return result
}

// getPublicIP 获取公网IP
func getPublicIP() string {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	apis := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
		"http://ipinfo.io/ip",
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
		if ip != "" && net.ParseIP(ip) != nil {
			return ip
		}
	}

	return ""
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Enabled:        false,
		DiscoveryPort:  19000,
		AdvertiseIP:    "",
		AdvertisePort:  0,
		Interval:       30 * time.Second,
		BootstrapNodes: []string{},
	}
}
