package cluster

import (
    "sync"
    "time"
    "log"
    "fmt"
    "encoding/json"
    "net/http"
)

// HealthChecker 健康检查器
type HealthChecker struct {
    interval    time.Duration
    nodes       map[string]*NodeHealth
    mutex       sync.RWMutex
    stopChan    chan struct{}
    alertChan   chan string // 用于发送告警信息
    manager     *ShardManager // 分片管理器引用
    metrics     *ClusterMetrics // 集群指标
    apiEnabled  bool // 是否启用API
}

// NodeHealth 节点健康状态
type NodeHealth struct {
    LastHeartbeat  time.Time
    Status         string
    Failures       int
    ResponseTime   time.Duration
    CPU            float64 // CPU使用率
    Memory         float64 // 内存使用率
    DiskUsage      float64 // 磁盘使用率
    NetworkIO      int64   // 网络IO
    ShardCount     int     // 分片数量
}

// ClusterMetrics 集群指标
type ClusterMetrics struct {
    TotalNodes     int
    HealthyNodes   int
    TotalShards    int
    ReplicaFactor  int
    QPS            int64
    AvgLatency     time.Duration
    mutex          sync.RWMutex
}

// NewHealthChecker 创建新的健康检查器
func NewHealthChecker(interval time.Duration, manager *ShardManager) *HealthChecker {
    return &HealthChecker{
        interval:   interval,
        nodes:      make(map[string]*NodeHealth),
        stopChan:   make(chan struct{}),
        alertChan:  make(chan string, 100), // 缓冲区大小为100
        manager:    manager,
        metrics:    &ClusterMetrics{},
        apiEnabled: true,
    }
}

// StartHealthCheck 启动健康检查
func (hc *HealthChecker) StartHealthCheck() {
    ticker := time.NewTicker(hc.interval)
    defer ticker.Stop()

    // 启动告警处理协程
    go hc.handleAlerts()
    
    // 如果启用API，启动HTTP服务
    if hc.apiEnabled {
        go hc.startAPIServer()
    }

    for {
        select {
        case <-ticker.C:
            hc.checkAllNodes()
            hc.updateClusterMetrics()
        case <-hc.stopChan:
            return
        }
    }
}

// checkAllNodes 检查所有节点
func (hc *HealthChecker) checkAllNodes() {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()

    for nodeID, health := range hc.nodes {
        if time.Since(health.LastHeartbeat) > hc.interval*3 {
            health.Status = "unhealthy"
            health.Failures++
            
            // 触发故障恢复
            if health.Failures >= 3 {
                go hc.triggerFailover(nodeID)
                hc.alertChan <- fmt.Sprintf("Node %s is unhealthy, triggering failover", nodeID)
            }
        } else {
            health.Status = "healthy"
            health.Failures = 0
        }
    }
}

// triggerFailover 触发故障恢复
func (hc *HealthChecker) triggerFailover(nodeID string) {
    log.Printf("Triggering failover for node %s", nodeID)
    
    // 获取节点上的分片
    if hc.manager != nil {
        if err := hc.manager.AutoFailover(nodeID); err != nil {
            log.Printf("Failover failed for node %s: %v", nodeID, err)
            hc.alertChan <- fmt.Sprintf("Failover failed for node %s: %v", nodeID, err)
        } else {
            log.Printf("Failover successful for node %s", nodeID)
            hc.alertChan <- fmt.Sprintf("Failover successful for node %s", nodeID)
        }
    }
}

// handleAlerts 处理告警信息
func (hc *HealthChecker) handleAlerts() {
    for alert := range hc.alertChan {
        log.Printf("ALERT: %s", alert)
        // 这里可以添加发送告警到外部系统的逻辑，如邮件、短信等
    }
}

// updateClusterMetrics 更新集群指标
func (hc *HealthChecker) updateClusterMetrics() {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    hc.metrics.mutex.Lock()
    defer hc.metrics.mutex.Unlock()
    
    hc.metrics.TotalNodes = len(hc.nodes)
    
    healthyNodes := 0
    for _, health := range hc.nodes {
        if health.Status == "healthy" {
            healthyNodes++
        }
    }
    
    hc.metrics.HealthyNodes = healthyNodes
    
    // 更新分片相关指标
    if hc.manager != nil {
        hc.metrics.TotalShards = hc.manager.GetTotalShards()
        hc.metrics.ReplicaFactor = hc.manager.GetReplicaFactor()
    }
    
    // 这里可以添加更多指标的收集逻辑
}

// startAPIServer 启动API服务器
func (hc *HealthChecker) startAPIServer() {
    // 注册API路由
    http.HandleFunc("/api/cluster/status", hc.handleClusterStatus)
    http.HandleFunc("/api/cluster/nodes", hc.handleNodesStatus)
    http.HandleFunc("/api/cluster/metrics", hc.handleClusterMetrics)
    http.HandleFunc("/api/cluster/parameters", hc.handleClusterParameters)
    
    // 启动HTTP服务
    log.Printf("Starting API server on :8080")
    if err := http.ListenAndServe(":8080", nil); err != nil {
        log.Printf("API server error: %v", err)
    }
}

// handleClusterStatus 处理集群状态API请求
func (hc *HealthChecker) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.metrics.mutex.RLock()
    defer hc.metrics.mutex.RUnlock()
    
    response := map[string]interface{}{
        "totalNodes": hc.metrics.TotalNodes,
        "healthyNodes": hc.metrics.HealthyNodes,
        "totalShards": hc.metrics.TotalShards,
        "replicaFactor": hc.metrics.ReplicaFactor,
        "status": hc.getClusterStatus(),
        "timestamp": time.Now(),
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleNodesStatus 处理节点状态API请求
func (hc *HealthChecker) handleNodesStatus(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    response := make(map[string]interface{})
    nodes := make([]map[string]interface{}, 0, len(hc.nodes))
    
    for nodeID, health := range hc.nodes {
        nodeInfo := map[string]interface{}{
            "id": nodeID,
            "status": health.Status,
            "lastHeartbeat": health.LastHeartbeat,
            "responseTime": health.ResponseTime.Milliseconds(),
            "cpu": health.CPU,
            "memory": health.Memory,
            "diskUsage": health.DiskUsage,
            "networkIO": health.NetworkIO,
            "shardCount": health.ShardCount,
        }
        nodes = append(nodes, nodeInfo)
    }
    
    response["nodes"] = nodes
    response["timestamp"] = time.Now()
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleClusterMetrics 处理集群指标API请求
func (hc *HealthChecker) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.metrics.mutex.RLock()
    defer hc.metrics.mutex.RUnlock()
    
    response := map[string]interface{}{
        "qps": hc.metrics.QPS,
        "avgLatency": hc.metrics.AvgLatency.Milliseconds(),
        "timestamp": time.Now(),
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleClusterParameters 处理集群参数API请求
func (hc *HealthChecker) handleClusterParameters(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodGet {
        // 获取参数
        hc.getClusterParameters(w, r)
    } else if r.Method == http.MethodPost {
        // 更新参数
        hc.updateClusterParameters(w, r)
    } else {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

// getClusterParameters 获取集群参数
func (hc *HealthChecker) getClusterParameters(w http.ResponseWriter, r *http.Request) {
    // 这里需要从配置管理器获取参数
    // 示例参数
    params := map[string]interface{}{
        "replicaFactor": 3,
        "heartbeatInterval": hc.interval.Seconds(),
        "failoverThreshold": 3,
        "shardCount": hc.metrics.TotalShards,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(params)
}

// updateClusterParameters 更新集群参数
func (hc *HealthChecker) updateClusterParameters(w http.ResponseWriter, r *http.Request) {
    var params map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }
    
    // 这里需要更新配置管理器中的参数
    // 示例：更新心跳间隔
    if interval, ok := params["heartbeatInterval"]; ok {
        if intervalFloat, ok := interval.(float64); ok {
            hc.interval = time.Duration(intervalFloat) * time.Second
            log.Printf("Updated heartbeat interval to %v", hc.interval)
        }
    }
    
    // 返回更新后的参数
    hc.getClusterParameters(w, r)
}

// getClusterStatus 获取集群整体状态
func (hc *HealthChecker) getClusterStatus() string {
    if hc.metrics.HealthyNodes < hc.metrics.TotalNodes/2 {
        return "critical"
    } else if hc.metrics.HealthyNodes < hc.metrics.TotalNodes {
        return "warning"
    }
    return "healthy"
}

// RegisterNode 注册节点
func (hc *HealthChecker) RegisterNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    // 如果节点已存在，更新心跳时间
    if health, exists := hc.nodes[nodeID]; exists {
        health.LastHeartbeat = time.Now()
        health.Status = "healthy"
        health.Failures = 0
        return
    }
    
    // 创建新节点健康记录
    hc.nodes[nodeID] = &NodeHealth{
        LastHeartbeat: time.Now(),
        Status:        "healthy",
        Failures:      0,
        ResponseTime:  0,
        CPU:           0,
        Memory:        0,
        DiskUsage:     0,
        NetworkIO:     0,
        ShardCount:    0,
    }
    
    log.Printf("Node %s registered", nodeID)
    
    // 更新集群指标
    hc.updateClusterMetrics()
}

// UpdateNodeMetrics 更新节点指标
func (hc *HealthChecker) UpdateNodeMetrics(nodeID string, metrics *NodeHealth) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    if health, exists := hc.nodes[nodeID]; exists {
        health.LastHeartbeat = time.Now()
        health.CPU = metrics.CPU
        health.Memory = metrics.Memory
        health.DiskUsage = metrics.DiskUsage
        health.NetworkIO = metrics.NetworkIO
        health.ShardCount = metrics.ShardCount
        health.ResponseTime = metrics.ResponseTime
    }
}

// RemoveNode 移除节点
func (hc *HealthChecker) RemoveNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    delete(hc.nodes, nodeID)
    log.Printf("Node %s removed", nodeID)
    
    // 更新集群指标
    hc.updateClusterMetrics()
}

// GetNodeHealth 获取节点健康状态
func (hc *HealthChecker) GetNodeHealth(nodeID string) (*NodeHealth, bool) {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    health, exists := hc.nodes[nodeID]
    if !exists {
        return nil, false
    }
    
    // 返回副本，避免并发修改
    return &NodeHealth{
        LastHeartbeat: health.LastHeartbeat,
        Status:        health.Status,
        Failures:      health.Failures,
        ResponseTime:  health.ResponseTime,
        CPU:           health.CPU,
        Memory:        health.Memory,
        DiskUsage:     health.DiskUsage,
        NetworkIO:     health.NetworkIO,
        ShardCount:    health.ShardCount,
    }, true
}

// GetAllNodesHealth 获取所有节点健康状态
func (hc *HealthChecker) GetAllNodesHealth() map[string]*NodeHealth {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    result := make(map[string]*NodeHealth)
    for nodeID, health := range hc.nodes {
        result[nodeID] = &NodeHealth{
            LastHeartbeat: health.LastHeartbeat,
            Status:        health.Status,
            Failures:      health.Failures,
            ResponseTime:  health.ResponseTime,
            CPU:           health.CPU,
            Memory:        health.Memory,
            DiskUsage:     health.DiskUsage,
            NetworkIO:     health.NetworkIO,
            ShardCount:    health.ShardCount,
        }
    }
    
    return result
}

// EnableAutoFailover 启用自动故障转移
func (hc *HealthChecker) EnableAutoFailover(enabled bool) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    // 这里可以添加启用/禁用自动故障转移的逻辑
    log.Printf("Auto failover %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// PerformManualFailover 执行手动故障转移
func (hc *HealthChecker) PerformManualFailover(nodeID string, targetNodeID string) error {
    log.Printf("Performing manual failover from node %s to node %s", nodeID, targetNodeID)
    
    // 检查目标节点是否健康
    if health, exists := hc.GetNodeHealth(targetNodeID); !exists || health.Status != "healthy" {
        return fmt.Errorf("target node %s is not healthy or does not exist", targetNodeID)
    }
    
    // 检查源节点是否存在
    if _, exists := hc.GetNodeHealth(nodeID); !exists {
        return fmt.Errorf("source node %s does not exist", nodeID)
    }
    
    // 执行故障转移
    if hc.manager != nil {
        // 调用分片管理器执行故障转移
        if err := hc.manager.MigrateShards(nodeID, targetNodeID); err != nil {
            log.Printf("Manual failover failed: %v", err)
            hc.alertChan <- fmt.Sprintf("Manual failover from node %s to %s failed: %v", nodeID, targetNodeID, err)
            return err
        }
        
        log.Printf("Manual failover from node %s to %s completed successfully", nodeID, targetNodeID)
        hc.alertChan <- fmt.Sprintf("Manual failover from node %s to %s completed successfully", nodeID, targetNodeID)
        return nil
    }
    
    return fmt.Errorf("shard manager not initialized")
```
```go
package cluster

import (
    "sync"
    "time"
    "log"
    "fmt"
    "encoding/json"
    "net/http"
)

// HealthChecker 健康检查器
type HealthChecker struct {
    interval    time.Duration
    nodes       map[string]*NodeHealth
    mutex       sync.RWMutex
    stopChan    chan struct{}
    alertChan   chan string // 用于发送告警信息
    manager     *ShardManager // 分片管理器引用
    metrics     *ClusterMetrics // 集群指标
    apiEnabled  bool // 是否启用API
}

// NodeHealth 节点健康状态
type NodeHealth struct {
    LastHeartbeat  time.Time
    Status         string
    Failures       int
    ResponseTime   time.Duration
    CPU            float64 // CPU使用率
    Memory         float64 // 内存使用率
    DiskUsage      float64 // 磁盘使用率
    NetworkIO      int64   // 网络IO
    ShardCount     int     // 分片数量
}

// ClusterMetrics 集群指标
type ClusterMetrics struct {
    TotalNodes     int
    HealthyNodes   int
    TotalShards    int
    ReplicaFactor  int
    QPS            int64
    AvgLatency     time.Duration
    mutex          sync.RWMutex
}

// NewHealthChecker 创建新的健康检查器
func NewHealthChecker(interval time.Duration, manager *ShardManager) *HealthChecker {
    return &HealthChecker{
        interval:   interval,
        nodes:      make(map[string]*NodeHealth),
        stopChan:   make(chan struct{}),
        alertChan:  make(chan string, 100), // 缓冲区大小为100
        manager:    manager,
        metrics:    &ClusterMetrics{},
        apiEnabled: true,
    }
}

// StartHealthCheck 启动健康检查
func (hc *HealthChecker) StartHealthCheck() {
    ticker := time.NewTicker(hc.interval)
    defer ticker.Stop()

    // 启动告警处理协程
    go hc.handleAlerts()
    
    // 如果启用API，启动HTTP服务
    if hc.apiEnabled {
        go hc.startAPIServer()
    }

    for {
        select {
        case <-ticker.C:
            hc.checkAllNodes()
            hc.updateClusterMetrics()
        case <-hc.stopChan:
            return
        }
    }
}

// checkAllNodes 检查所有节点
func (hc *HealthChecker) checkAllNodes() {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()

    for nodeID, health := range hc.nodes {
        if time.Since(health.LastHeartbeat) > hc.interval*3 {
            health.Status = "unhealthy"
            health.Failures++
            
            // 触发故障恢复
            if health.Failures >= 3 {
                go hc.triggerFailover(nodeID)
                hc.alertChan <- fmt.Sprintf("Node %s is unhealthy, triggering failover", nodeID)
            }
        } else {
            health.Status = "healthy"
            health.Failures = 0
        }
    }
}

// triggerFailover 触发故障恢复
func (hc *HealthChecker) triggerFailover(nodeID string) {
    log.Printf("Triggering failover for node %s", nodeID)
    
    // 获取节点上的分片
    if hc.manager != nil {
        if err := hc.manager.AutoFailover(nodeID); err != nil {
            log.Printf("Failover failed for node %s: %v", nodeID, err)
            hc.alertChan <- fmt.Sprintf("Failover failed for node %s: %v", nodeID, err)
        } else {
            log.Printf("Failover successful for node %s", nodeID)
            hc.alertChan <- fmt.Sprintf("Failover successful for node %s", nodeID)
        }
    }
}

// handleAlerts 处理告警信息
func (hc *HealthChecker) handleAlerts() {
    for alert := range hc.alertChan {
        log.Printf("ALERT: %s", alert)
        // 这里可以添加发送告警到外部系统的逻辑，如邮件、短信等
    }
}

// updateClusterMetrics 更新集群指标
func (hc *HealthChecker) updateClusterMetrics() {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    hc.metrics.mutex.Lock()
    defer hc.metrics.mutex.Unlock()
    
    hc.metrics.TotalNodes = len(hc.nodes)
    
    healthyNodes := 0
    for _, health := range hc.nodes {
        if health.Status == "healthy" {
            healthyNodes++
        }
    }
    
    hc.metrics.HealthyNodes = healthyNodes
    
    // 更新分片相关指标
    if hc.manager != nil {
        hc.metrics.TotalShards = hc.manager.GetTotalShards()
        hc.metrics.ReplicaFactor = hc.manager.GetReplicaFactor()
    }
    
    // 这里可以添加更多指标的收集逻辑
}

// startAPIServer 启动API服务器
func (hc *HealthChecker) startAPIServer() {
    // 注册API路由
    http.HandleFunc("/api/cluster/status", hc.handleClusterStatus)
    http.HandleFunc("/api/cluster/nodes", hc.handleNodesStatus)
    http.HandleFunc("/api/cluster/metrics", hc.handleClusterMetrics)
    http.HandleFunc("/api/cluster/parameters", hc.handleClusterParameters)
    
    // 启动HTTP服务
    log.Printf("Starting API server on :8080")
    if err := http.ListenAndServe(":8080", nil); err != nil {
        log.Printf("API server error: %v", err)
    }
}

// handleClusterStatus 处理集群状态API请求
func (hc *HealthChecker) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.metrics.mutex.RLock()
    defer hc.metrics.mutex.RUnlock()
    
    response := map[string]interface{}{
        "totalNodes": hc.metrics.TotalNodes,
        "healthyNodes": hc.metrics.HealthyNodes,
        "totalShards": hc.metrics.TotalShards,
        "replicaFactor": hc.metrics.ReplicaFactor,
        "status": hc.getClusterStatus(),
        "timestamp": time.Now(),
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleNodesStatus 处理节点状态API请求
func (hc *HealthChecker) handleNodesStatus(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    response := make(map[string]interface{})
    nodes := make([]map[string]interface{}, 0, len(hc.nodes))
    
    for nodeID, health := range hc.nodes {
        nodeInfo := map[string]interface{}{
            "id": nodeID,
            "status": health.Status,
            "lastHeartbeat": health.LastHeartbeat,
            "responseTime": health.ResponseTime.Milliseconds(),
            "cpu": health.CPU,
            "memory": health.Memory,
            "diskUsage": health.DiskUsage,
            "networkIO": health.NetworkIO,
            "shardCount": health.ShardCount,
        }
        nodes = append(nodes, nodeInfo)
    }
    
    response["nodes"] = nodes
    response["timestamp"] = time.Now()
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleClusterMetrics 处理集群指标API请求
func (hc *HealthChecker) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    hc.metrics.mutex.RLock()
    defer hc.metrics.mutex.RUnlock()
    
    response := map[string]interface{}{
        "qps": hc.metrics.QPS,
        "avgLatency": hc.metrics.AvgLatency.Milliseconds(),
        "timestamp": time.Now(),
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// handleClusterParameters 处理集群参数API请求
func (hc *HealthChecker) handleClusterParameters(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodGet {
        // 获取参数
        hc.getClusterParameters(w, r)
    } else if r.Method == http.MethodPost {
        // 更新参数
        hc.updateClusterParameters(w, r)
    } else {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

// getClusterParameters 获取集群参数
func (hc *HealthChecker) getClusterParameters(w http.ResponseWriter, r *http.Request) {
    // 这里需要从配置管理器获取参数
    // 示例参数
    params := map[string]interface{}{
        "replicaFactor": 3,
        "heartbeatInterval": hc.interval.Seconds(),
        "failoverThreshold": 3,
        "shardCount": hc.metrics.TotalShards,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(params)
}

// updateClusterParameters 更新集群参数
func (hc *HealthChecker) updateClusterParameters(w http.ResponseWriter, r *http.Request) {
    var params map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }
    
    // 这里需要更新配置管理器中的参数
    // 示例：更新心跳间隔
    if interval, ok := params["heartbeatInterval"]; ok {
        if intervalFloat, ok := interval.(float64); ok {
            hc.interval = time.Duration(intervalFloat) * time.Second
            log.Printf("Updated heartbeat interval to %v", hc.interval)
        }
    }
    
    // 返回更新后的参数
    hc.getClusterParameters(w, r)
}

// getClusterStatus 获取集群整体状态
func (hc *HealthChecker) getClusterStatus() string {
    if hc.metrics.HealthyNodes < hc.metrics.TotalNodes/2 {
        return "critical"
    } else if hc.metrics.HealthyNodes < hc.metrics.TotalNodes {
        return "warning"
    }
    return "healthy"
}

// RegisterNode 注册节点
func (hc *HealthChecker) RegisterNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    // 如果节点已存在，更新心跳时间
    if health, exists := hc.nodes[nodeID]; exists {
        health.LastHeartbeat = time.Now()
        health.Status = "healthy"
        health.Failures = 0
        return
    }
    
    // 创建新节点健康记录
    hc.nodes[nodeID] = &NodeHealth{
        LastHeartbeat: time.Now(),
        Status:        "healthy",
        Failures:      0,
        ResponseTime:  0,
        CPU:           0,
        Memory:        0,
        DiskUsage:     0,
        NetworkIO:     0,
        ShardCount:    0,
    }
    
    log.Printf("Node %s registered", nodeID)
    
    // 更新集群指标
    hc.updateClusterMetrics()
}

// UpdateNodeMetrics 更新节点指标
func (hc *HealthChecker) UpdateNodeMetrics(nodeID string, metrics *NodeHealth) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    if health, exists := hc.nodes[nodeID]; exists {
        health.LastHeartbeat = time.Now()
        health.CPU = metrics.CPU
        health.Memory = metrics.Memory
        health.DiskUsage = metrics.DiskUsage
        health.NetworkIO = metrics.NetworkIO
        health.ShardCount = metrics.ShardCount
        health.ResponseTime = metrics.ResponseTime
    }
}

// RemoveNode 移除节点
func (hc *HealthChecker) RemoveNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    delete(hc.nodes, nodeID)
    log.Printf("Node %s removed", nodeID)
    
    // 更新集群指标
    hc.updateClusterMetrics()
}

// GetNodeHealth 获取节点健康状态
func (hc *HealthChecker) GetNodeHealth(nodeID string) (*NodeHealth, bool) {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    health, exists := hc.nodes[nodeID]
    if !exists {
        return nil, false
    }
    
    // 返回副本，避免并发修改
    return &NodeHealth{
        LastHeartbeat: health.LastHeartbeat,
        Status:        health.Status,
        Failures:      health.Failures,
        ResponseTime:  health.ResponseTime,
        CPU:           health.CPU,
        Memory:        health.Memory,
        DiskUsage:     health.DiskUsage,
        NetworkIO:     health.NetworkIO,
        ShardCount:    health.ShardCount,
    }, true
}

// GetAllNodesHealth 获取所有节点健康状态
func (hc *HealthChecker) GetAllNodesHealth() map[string]*NodeHealth {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    result := make(map[string]*NodeHealth)
    for nodeID, health := range hc.nodes {
        result[nodeID] = &NodeHealth{
            LastHeartbeat: health.LastHeartbeat,
            Status:        health.Status,
            Failures:      health.Failures,
            ResponseTime:  health.ResponseTime,
            CPU:           health.CPU,
            Memory:        health.Memory,
            DiskUsage:     health.DiskUsage,
            NetworkIO:     health.NetworkIO,
            ShardCount:    health.ShardCount,
        }
    }
    
    return result
}

// EnableAutoFailover 启用自动故障转移
func (hc *HealthChecker) EnableAutoFailover(enabled bool) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    // 这里可以添加启用/禁用自动故障转移的逻辑
    log.Printf("Auto failover %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// PerformManualFailover 执行手动故障转移
func (hc *HealthChecker) PerformManualFailover(nodeID string, targetNodeID string) error {
    log.Printf("Performing manual failover from node %s to node %s", nodeID, targetNodeID)
    
    // 检查目标节点是否健康
    if health, exists := hc.GetNodeHealth(targetNodeID); !exists || health.Status != "healthy" {
        return fmt.Errorf("target node %s is not healthy or does not exist", targetNodeID)
    }
    
    // 检查源节点是否存在
    if _, exists := hc.GetNodeHealth(nodeID); !exists {
        return fmt.Errorf("source node %s does not exist", nodeID)
    }
    
    // 执行故障转移
    if hc.manager != nil {
        // 调用分片管理器执行故障转移
        if err := hc.manager.MigrateShards(nodeID, targetNodeID); err != nil {
            log.Printf("Manual failover failed: %v", err)
            hc.alertChan <- fmt.Sprintf("Manual failover from node %s to %s failed: %v", nodeID, targetNodeID, err)
            return err
        }
        
        log.Printf("Manual failover from node %s to %s completed successfully", nodeID, targetNodeID)
        hc.alertChan <- fmt.Sprintf("Manual failover from node %s to %s completed successfully", nodeID, targetNodeID)
        return nil
    }
    
    return fmt.Errorf("shard manager not initialized")