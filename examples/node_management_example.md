# 节点管理示例

本文档展示了如何在Cyber-DB数据库中管理集群节点。

## 节点启动和配置

### 1. 启动主节点

```bash
# 启动主节点，指定节点ID和集群配置
./godata -host 127.0.0.1 -port 3306 \
        -node-id master \
        -cluster-mode true \
        -raft-dir ./raft-master
```

### 2. 添加从节点

```bash
# 启动从节点1
./godata -host 127.0.0.1 -port 3307 \
        -node-id slave1 \
        -cluster-mode true \
        -join-addr 127.0.0.1:3306 \
        -raft-dir ./raft-slave1

# 启动从节点2
./godata -host 127.0.0.1 -port 3308 \
        -node-id slave2 \
        -cluster-mode true \
        -join-addr 127.0.0.1:3306 \
        -raft-dir ./raft-slave2
```

## 节点管理示例

### 1. 节点状态检查

```go
// 获取所有节点信息
peers := node.GetPeers()
for _, peer := range peers {
    fmt.Printf("节点ID: %s\n", peer.ID)
    fmt.Printf("状态: %s\n", peer.State)
    fmt.Printf("地址: %s:%d\n", peer.Address, peer.Port)
    fmt.Printf("最后心跳时间: %s\n\n", peer.LastSeen)
}
```

### 2. 添加新节点

```go
// 创建新节点信息
newPeer := &NodeInfo{
    ID:      "node4",
    State:   StateActive,
    Address: "127.0.0.1",
    Port:    3309,
}

// 添加到集群
node.AddPeer(newPeer)

// 等待节点同步
time.Sleep(5 * time.Second)

// 验证节点状态
if peer := node.GetPeer(newPeer.ID); peer != nil {
    fmt.Printf("新节点已加入集群: %s\n", peer.ID)
}
```

### 3. 处理节点故障

```go
// 监控节点状态
go func() {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for range ticker.C {
        peers := node.GetPeers()
        for _, peer := range peers {
            // 检查节点是否存活
            if time.Since(peer.LastSeen) > 10*time.Second {
                fmt.Printf("节点可能故障: %s\n", peer.ID)
                // 更新节点状态
                node.updatePeerState(peer.ID, StateFailed)
                // 触发故障处理
                handleNodeFailure(peer.ID)
            }
        }
    }
}()

// 故障处理函数
func handleNodeFailure(nodeID string) {
    // 从集群中移除故障节点
    node.RemovePeer(nodeID)
    
    // 重新分配分片
    shardManager.HandleNodeFailure(nodeID)
    
    // 通知其他节点
    node.broadcastNodeInfo()
}
```

### 4. 节点恢复

```go
// 节点恢复处理
func handleNodeRecovery(nodeID string) error {
    // 检查节点是否可访问
    if !checkNodeAvailable(nodeID) {
        return errors.New("节点仍不可访问")
    }

    // 重新添加到集群
    peer := &NodeInfo{
        ID:      nodeID,
        State:   StateActive,
        Address: getNodeAddress(nodeID),
        Port:    getNodePort(nodeID),
    }
    node.AddPeer(peer)

    // 同步数据
    if err := syncNodeData(nodeID); err != nil {
        return fmt.Errorf("数据同步失败: %v", err)
    }

    return nil
}
```

## 最佳实践

1. 节点配置
   - 合理配置节点参数
     * 设置适当的内存限制
     * 配置合适的网络参数
     * 优化磁盘I/O设置
   - 使用持久化的Raft目录
     * 选择可靠的存储设备
     * 定期备份Raft状态
     * 配置日志清理策略
   - 设置适当的超时时间
     * 心跳间隔：1-3秒
     * 选举超时：5-10秒
     * 操作超时：根据业务需求设置

2. 故障处理
   - 实现自动故障检测
   - 及时清理失效节点
   - 保持数据一致性

3. 监控和维护
   - 定期检查节点状态
   - 监控节点负载
   - 记录重要操作日志

4. 扩展性考虑
   - 预留足够的资源
   - 平滑扩容和缩容
   - 负载均衡策略

### 3. 集群健康检查

```go
// 获取集群健康状态
resp, err := http.Get("http://localhost:8080/api/cluster/status")
if err == nil {
    defer resp.Body.Close()
    var status map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&status)
    fmt.Printf("集群状态: %s\n健康节点数: %d\n总分片数: %d\n", 
        status["status"], status["healthyNodes"], status["totalShards"])
}

// 更新心跳间隔配置
params := map[string]interface{}{"heartbeatInterval": 5}
body, _ := json.Marshal(params)
req, _ := http.NewRequest("POST", "http://localhost:8080/api/cluster/parameters", bytes.NewBuffer(body))
client := &http.Client{}
resp, err = client.Do(req)
if err == nil {
    defer resp.Body.Close()
    fmt.Println("配置更新成功")
}
```

## 注意事项

3. 推荐配置参数：
   - 心跳间隔：5秒
   - 故障转移阈值：3次失败
   - 节点健康检查超时：15秒
   - 最大网络延迟：200ms

1. 确保节点之间的网络连接稳定
2. 合理设置心跳超时时间
3. 实现适当的故障恢复机制
4. 注意数据同步和一致性
5. 定期备份重要数据
6. 监控节点资源使用情况