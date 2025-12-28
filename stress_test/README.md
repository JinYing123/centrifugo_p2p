# Centrifugo P2P Broker 压力测试

## 编译

```bash
cd stress_test

# 编译 Linux 版本
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o stress ./cmd/stress
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o leaktest ./cmd/leaktest

# 编译本地版本
go build -o stress ./cmd/stress
go build -o leaktest ./cmd/leaktest
```

## 测试脚本

### 1. 发布性能测试 (`stress`)

测试消息发布的性能，统计延迟和吞吐量。

```bash
# 运行（默认参数）
./stress

# 自定义参数
./stress \
  -url http://your-server:8000 \
  -messages 5000 \
  -concurrency 20 \
  -channel "self:test"
```

参数说明：
- `-url`: Centrifugo 服务地址
- `-key`: API Key
- `-messages`: 发送的消息数量
- `-concurrency`: 并发工作者数量
- `-channel`: 目标频道

### 2. Goroutine 泄漏测试 (`leaktest`)

模拟频繁的订阅/取消订阅操作，检测是否有 goroutine 泄漏。

```bash
# 运行（默认参数）
./leaktest

# 自定义参数
./leaktest \
  -url http://your-server:8000 \
  -iterations 500
```

参数说明：
- `-url`: Centrifugo 服务地址
- `-key`: API Key
- `-iterations`: 订阅/取消订阅循环次数

## 如何对比新旧代码

### 方法 1：使用旧代码运行测试

1. 部署旧版本的 centrifugo
2. 运行压力测试并记录结果
3. 运行 goroutine 泄漏测试

### 方法 2：使用新代码运行测试

1. 部署新版本的 centrifugo
2. 运行相同的测试
3. 对比结果

### 关注指标

1. **发布测试**
   - 平均延迟 (Avg)
   - P99 延迟
   - 吞吐量 (msg/s)

2. **Goroutine 泄漏测试**
   - `topics_count` 是否持续增长
   - 测试结束后 topics 是否被清理

## 预期结果

### 旧代码（有 bug）

- Goroutine 泄漏测试后 `topics_count` 可能异常增长
- 长时间运行后性能下降

### 新代码（修复后）

- `topics_count` 在测试结束后应该回归正常
- 性能应该稳定

## 服务器监控命令

在运行测试时，同时在服务器上监控：

```bash
# 监控 centrifugo 进程
watch -n 1 'ps aux | grep centrifugo'

# 监控内存和 goroutine（如果开启了 pprof）
curl http://localhost:8000/debug/pprof/goroutine?debug=1 | head -10

# 监控连接数
watch -n 1 'netstat -an | grep :8000 | grep ESTABLISHED | wc -l'
```

