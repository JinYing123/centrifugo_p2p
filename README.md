# Centrifugo P2P

基于 [Centrifugo v6](https://github.com/centrifugal/centrifugo) 的 P2P 改造版本，使用 **libp2p + GossipSub** 替代 Redis 实现分布式消息 Broker，让 Centrifugo 集群**无需外部依赖**即可自组网运行。

## P2P 改造概述

原版 Centrifugo 的多节点集群依赖 Redis/Nats 作为消息 Broker。本项目通过 P2P 网络替代中心化的 Broker，实现：

- **去中心化集群** — 节点之间通过 libp2p 直连，无需 Redis
- **自动节点发现** — 支持 Bootstrap 引导节点 + mDNS 局域网发现
- **GossipSub 消息分发** — 频道消息通过 GossipSub 协议在节点间传播
- **客户端智能路由** — 前端可通过 P2P API 发现所有节点并选择最低延迟的服务器

## 架构

```
┌──────────────────────────────────────────────────────────┐
│                    Client (Frontend)                      │
│  1. GET /p2p/nodes → 获取所有节点列表                       │
│  2. GET /p2p/latency → 测速选择最优节点                     │
│  3. WebSocket 连接到最优节点                                │
└────────────────────┬─────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ Node A  │ │ Node B  │ │ Node C  │
   │ :8000   │ │ :8000   │ │ :8000   │
   │ :4001   │ │ :4001   │ │ :4001   │
   └────┬────┘ └────┬────┘ └────┬────┘
        │            │            │
        └────── libp2p ───────────┘
            GossipSub 消息同步
```

## 新增模块

| 模块 | 路径 | 说明 |
|------|------|------|
| **P2P Broker** | `internal/p2pbroker/` | 实现 `centrifuge.Broker` 接口，基于 libp2p + GossipSub |
| **P2P Discovery** | `internal/p2p/` | UDP 节点发现服务（心跳、注册、引导节点） |
| **P2P API** | `internal/p2papi/` | HTTP API，提供节点状态、Peer 管理、延迟测试等 |
| **压力测试** | `stress_test/` | 发布性能测试 + Goroutine 泄漏检测 |

## 快速开始

### 1. 编译

```bash
go build -o centrifugo .
```

### 2. 配置

在 `config.json` 中启用 P2P Broker：

```json
{
  "broker": {
    "enabled": true,
    "type": "p2p"
  },
  "p2p_broker": {
    "enabled": true,
    "listen_port": 4001,
    "advertise_ip": "",
    "bootstrap_nodes": [],
    "topic_prefix": "centrifugo",
    "enable_mdns": true,
    "identity_key_file": "/var/opt/p2p_identity.key"
  }
}
```

### 3. 运行

**单节点（开发模式，mDNS 自动发现）：**

```bash
./centrifugo --config config.json
```

**多节点集群（生产模式）：**

```bash
# 节点 A（引导节点）
./centrifugo --config config.json

# 节点 B（指定引导节点）
# 在 config.json 的 p2p_broker.bootstrap_nodes 中添加节点 A 的地址：
# ["/ip4/<节点A的IP>/tcp/4001/p2p/<节点A的PeerID>"]
./centrifugo --config config.json
```

> 启动后日志会输出本节点的 Peer ID 和监听地址，可用作其他节点的 bootstrap 地址。

## P2P API

### 管理 API（需 API Key）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/p2p/status` | GET | 节点 P2P 状态（Peer ID、连接数、Topic 数等） |
| `/p2p/peers` | GET | 已连接 Peer 详情（地址、延迟、协议） |
| `/p2p/ping` | GET/POST | Ping 所有或指定 Peer |
| `/p2p/addrs` | GET | 本节点的 multiaddr 地址 |
| `/p2p/refresh` | POST | 触发节点发现刷新 |

### 客户端 API（无需认证）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/p2p/nodes` | GET | 获取所有可用 Centrifugo 节点列表（含 WebSocket 地址） |
| `/p2p/latency` | GET | 延迟测试端点，前端用于测速选择最优节点 |

## 配置参考

### P2P Broker (`p2p_broker`)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | false | 启用 P2P Broker |
| `listen_port` | int | 4001 | libp2p 监听端口 |
| `advertise_ip` | string | 自动获取 | 对外广播的 IP |
| `bootstrap_nodes` | []string | [] | 引导节点地址列表 |
| `topic_prefix` | string | "centrifugo" | GossipSub Topic 前缀 |
| `enable_mdns` | bool | true | 启用局域网 mDNS 发现 |
| `identity_key_file` | string | "" | 私钥文件路径，固定 Peer ID |

### P2P Discovery (`p2p`)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | false | 启用 UDP 节点发现 |
| `discovery_port` | int | 19000 | UDP 发现端口 |
| `advertise_ip` | string | 自动获取 | 对外广播的 IP |
| `advertise_port` | int | 同 HTTP 端口 | 广播的服务端口 |
| `interval` | int | 30 | 心跳间隔（秒） |
| `bootstrap_nodes` | []string | [] | 引导节点（`ip:port`） |

## 压力测试

详见 [stress_test/README.md](stress_test/README.md)

```bash
cd stress_test

# 编译
go build -o stress ./cmd/stress
go build -o leaktest ./cmd/leaktest

# 发布性能测试
./stress -url http://your-server:8000 -messages 5000 -concurrency 20

# Goroutine 泄漏测试
./leaktest -url http://your-server:8000 -iterations 500
```

## 与原版 Centrifugo 的区别

| 特性 | 原版 Centrifugo | P2P 版本 |
|------|----------------|----------|
| 集群 Broker | Redis / Nats | libp2p + GossipSub |
| 外部依赖 | 需要 Redis/Nats | 无需任何外部服务 |
| 节点发现 | 手动配置 | 自动发现（mDNS + Bootstrap） |
| 消息历史 | 支持（Redis 持久化） | 不支持（纯内存） |
| 客户端路由 | 自行实现 | 内置 nodes/latency API |
| 适用场景 | 大规模生产环境 | 中小规模、快速部署、边缘计算 |

## 许可证

同原版 Centrifugo，详见 [LICENSE](LICENSE)。
