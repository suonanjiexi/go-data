package cluster

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"
    
    "github.com/suonanjiexi/cyber-db/pkg/metrics"
)

// NodeState 表示节点状态
type NodeState string

const (
    NodeStateUnknown NodeState = "unknown"
    NodeStateUp      NodeState = "up"
    NodeStateDown    NodeState = "down"
    NodeStateDegraded NodeState = "degraded"
    NodeStateMaintenance NodeState = "maintenance"
)

// NodeMetrics 节点指标
type NodeMetrics struct {
    CPU            float64
    Memory         float64
    DiskUsage      float64
    NetworkIO      float64
    ResponseTime   time.Duration
}

// HealthChecker 集群健康检查器
type HealthChecker struct {
    nodes           map[string]*NodeHealth
    interval        time.Duration
    failureThreshold int
    recoveryThreshold int
    mutex           sync.RWMutex
    clusterManager  *ClusterManager
    stateChangeCh   chan StateChangeEvent
    metricCollector *metrics.Collector
    lastCheckTime   map[string]time.Time
    failureCount    map[string]int
    recoveryCount   map[string]int
    config          *HealthConfig
    ctx             context.Context
    cancel          context.CancelFunc
}

// HealthConfig 健康检查配置
type HealthConfig struct {
    HeartbeatInterval    time.Duration
    FailureThreshold     int
    RecoveryThreshold    int
    CPUThreshold         float64
    MemoryThreshold      float64
    DiskThreshold        float64
    NetworkLatencyThreshold time.Duration
    EnableSelfHealing    bool
}

// NodeHealth 节点健康状态
type NodeHealth struct {
    NodeID         string
    State          NodeState
    LastHeartbeat  time.Time
    CPU            float64
    Memory         float64
    DiskUsage      float64
    NetworkIO      float64
    ShardCount     int
    ResponseTime   time.Duration
    LastError      string
    LastErrorTime  time.Time
}

// StateChangeEvent 状态变更事件
type StateChangeEvent struct {
    NodeID    string
    OldState  NodeState
    NewState  NodeState
    Timestamp time.Time
    Reason    string
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(cm *ClusterManager, config *HealthConfig) *HealthChecker {
    if config == nil {
        config = &HealthConfig{
            HeartbeatInterval:    5 * time.Second,
            FailureThreshold:     3,
            RecoveryThreshold:    2,
            CPUThreshold:         80.0,
            MemoryThreshold:      85.0,
            DiskThreshold:        90.0,
            NetworkLatencyThreshold: 200 * time.Millisecond,
            EnableSelfHealing:    true,
        }
    }
    
    ctx, cancel := context.WithCancel(context.Background())
    
    return &HealthChecker{
        nodes:            make(map[string]*NodeHealth),
        interval:         config.HeartbeatInterval,
        failureThreshold: config.FailureThreshold,
        recoveryThreshold: config.RecoveryThreshold,
        clusterManager:   cm,
        stateChangeCh:    make(chan StateChangeEvent, 100),
        metricCollector:  metrics.NewCollector(),
        lastCheckTime:    make(map[string]time.Time),
        failureCount:     make(map[string]int),
        recoveryCount:    make(map[string]int),
        config:           config,
        ctx:              ctx,
        cancel:           cancel,
    }
}

// Start 启动健康检查
func (hc *HealthChecker) Start() {
    // 启动状态变更处理协程
    go hc.handleStateChanges()
    
    // 启动定期健康检查
    go func() {
        ticker := time.NewTicker(hc.interval)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                hc.checkAllNodes()
            case <-hc.ctx.Done():
                return
            }
        }
    }()
    
    log.Println("集群健康检查已启动")
}

// Stop 停止健康检查
func (hc *HealthChecker) Stop() {
    hc.cancel()
    close(hc.stateChangeCh)
    log.Println("集群健康检查已停止")
}

// RegisterNode 注册节点
func (hc *HealthChecker) RegisterNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    if _, exists := hc.nodes[nodeID]; !exists {
        hc.nodes[nodeID] = &NodeHealth{
            NodeID:        nodeID,
            State:         NodeStateUnknown,
            LastHeartbeat: time.Time{},
        }
        hc.failureCount[nodeID] = 0
        hc.recoveryCount[nodeID] = 0
        log.Printf("节点 %s 已注册到健康检查", nodeID)
    }
}

// UnregisterNode 注销节点
func (hc *HealthChecker) UnregisterNode(nodeID string) {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    if _, exists := hc.nodes[nodeID]; exists {
        delete(hc.nodes, nodeID)
        delete(hc.failureCount, nodeID)
        delete(hc.recoveryCount, nodeID)
        delete(hc.lastCheckTime, nodeID)
        log.Printf("节点 %s 已从健康检查注销", nodeID)
    }
}

// checkAllNodes 检查所有节点健康状态
func (hc *HealthChecker) checkAllNodes() {
    hc.mutex.RLock()
    nodeIDs := make([]string, 0, len(hc.nodes))
    for nodeID := range hc.nodes {
        nodeIDs = append(nodeIDs, nodeID)
    }
    hc.mutex.RUnlock()
    
    var wg sync.WaitGroup
    for _, nodeID := range nodeIDs {
        wg.Add(1)
        go func(id string) {
            defer wg.Done()
            hc.checkNodeHealth(id)
        }(nodeID)
    }
    
    wg.Wait()
}

// checkNodeHealth 检查单个节点健康状态
func (hc *HealthChecker) checkNodeHealth(nodeID string) {
    hc.mutex.RLock()
    node, exists := hc.nodes[nodeID]
    hc.mutex.RUnlock()
    
    if !exists {
        return
    }
    
    // 记录检查时间
    now := time.Now()
    hc.mutex.Lock()
    hc.lastCheckTime[nodeID] = now
    hc.mutex.Unlock()
    
    // 发送心跳请求
    healthy, metrics, err := hc.sendHeartbeat(nodeID)
    
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    // 更新节点状态
    oldState := node.State
    
    if !healthy {
        // 增加失败计数
        hc.failureCount[nodeID]++
        hc.recoveryCount[nodeID] = 0
        
        if err != nil {
            node.LastError = err.Error()
            node.LastErrorTime = now
        }
        
        // 检查是否达到故障阈值
        if hc.failureCount[nodeID] >= hc.failureThreshold && node.State != NodeStateDown {
            // 标记节点为故障状态
            node.State = NodeStateDown
            
            // 发送状态变更事件
            hc.sendStateChangeEvent(nodeID, oldState, NodeStateDown, fmt.Sprintf("连续 %d 次健康检查失败", hc.failureCount[nodeID]))
            
            // 触发故障转移
            if hc.config.EnableSelfHealing {
                go hc.clusterManager.HandleNodeFailure(nodeID)
            }
        }
    } else {
        // 更新节点指标
        if metrics != nil {
            node.CPU = metrics.CPU
            node.Memory = metrics.Memory
            node.DiskUsage = metrics.DiskUsage
            node.NetworkIO = metrics.NetworkIO
            node.ResponseTime = metrics.ResponseTime
        }
        
        // 重置失败计数，增加恢复计数
        hc.failureCount[nodeID] = 0
        hc.recoveryCount[nodeID]++
        
        // 更新最后心跳时间
        node.LastHeartbeat = now
        
        // 检查是否达到恢复阈值
        if hc.recoveryCount[nodeID] >= hc.recoveryThreshold && node.State == NodeStateDown {
            // 标记节点为正常状态
            node.State = NodeStateUp
            
            // 发送状态变更事件
            hc.sendStateChangeEvent(nodeID, oldState, NodeStateUp, fmt.Sprintf("连续 %d 次健康检查成功", hc.recoveryCount[nodeID]))
            
            // 触发节点恢复处理
            if hc.config.EnableSelfHealing {
                go hc.clusterManager.HandleNodeRecovery(nodeID)
            }
        }
        
        // 检查资源使用是否超过阈值
        if node.CPU > hc.config.CPUThreshold {
            hc.sendResourceAlert(nodeID, "CPU", node.CPU, hc.config.CPUThreshold)
        }
        
        if node.Memory > hc.config.MemoryThreshold {
            hc.sendResourceAlert(nodeID, "内存", node.Memory, hc.config.MemoryThreshold)
        }
        
        if node.DiskUsage > hc.config.DiskThreshold {
            hc.sendResourceAlert(nodeID, "磁盘", node.DiskUsage, hc.config.DiskThreshold)
        }
        
        if node.ResponseTime > hc.config.NetworkLatencyThreshold {
            hc.sendResourceAlert(nodeID, "网络延迟", float64(node.ResponseTime.Milliseconds()), float64(hc.config.NetworkLatencyThreshold.Milliseconds()))
        }
    }
}

// sendHeartbeat 发送心跳请求到节点
func (hc *HealthChecker) sendHeartbeat(nodeID string) (bool, *NodeMetrics, error) {
    // 获取节点连接信息
    node, err := hc.clusterManager.GetNodeInfo(nodeID)
    if err != nil {
        return false, nil, fmt.Errorf("获取节点信息失败: %v", err)
    }
    
    // 创建心跳请求上下文，带超时
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    
    // 发送心跳请求
    startTime := time.Now()
    metrics, err := hc.clusterManager.SendHeartbeat(ctx, node.Address)
    responseTime := time.Since(startTime)
    
    if err != nil {
        return false, nil, fmt.Errorf("心跳请求失败: %v", err)
    }
    
    // 添加响应时间
    metrics.ResponseTime = responseTime
    
    return true, metrics, nil
}

// sendStateChangeEvent 发送状态变更事件
func (hc *HealthChecker) sendStateChangeEvent(nodeID string, oldState, newState NodeState, reason string) {
    event := StateChangeEvent{
        NodeID:    nodeID,
        OldState:  oldState,
        NewState:  newState,
        Timestamp: time.Now(),
        Reason:    reason,
    }
    
    select {
    case hc.stateChangeCh <- event:
        // 事件已发送
    default:
        // 通道已满，记录日志
        log.Printf("警告: 状态变更事件通道已满，丢弃事件: %+v", event)
    }
}

// handleStateChanges 处理状态变更事件
func (hc *HealthChecker) handleStateChanges() {
    for {
        select {
        case event, ok := <-hc.stateChangeCh:
            if !ok {
                return // 通道已关闭
            }
            
            // 记录状态变更
            log.Printf("节点 %s 状态从 %s 变为 %s, 原因: %s", 
                event.NodeID, event.OldState, event.NewState, event.Reason)
            
            // 更新集群状态
            hc.clusterManager.UpdateNodeState(event.NodeID, event.NewState)
            
            // 发送告警通知
            hc.sendAlert(event)
            
        case <-hc.ctx.Done():
            return
        }
    }
}

// sendResourceAlert 发送资源告警
func (hc *HealthChecker) sendResourceAlert(nodeID string, resourceType string, value float64, threshold float64) {
    log.Printf("告警: 节点 %s 的%s使用率为 %.2f%%, 超过阈值 %.2f%%", 
        nodeID, resourceType, value, threshold)
    
    // 这里可以集成外部告警系统，如邮件、短信、Slack等
    // TODO: 实现告警集成
}

// sendAlert 发送节点状态变更告警
func (hc *HealthChecker) sendAlert(event StateChangeEvent) {
    // 根据状态变更类型发送不同级别的告警
    if event.NewState == NodeStateDown {
        log.Printf("严重告警: 节点 %s 已宕机，原因: %s", event.NodeID, event.Reason)
        // 发送高优先级告警
    } else if event.NewState == NodeStateDegraded {
        log.Printf("警告告警: 节点 %s 性能降级，原因: %s", event.NodeID, event.Reason)
        // 发送中优先级告警
    } else if event.OldState == NodeStateDown && event.NewState == NodeStateUp {
        log.Printf("恢复告警: 节点 %s 已恢复正常", event.NodeID)
        // 发送恢复通知
    }
    
    // 这里可以集成外部告警系统
    // TODO: 实现告警集成
}

// GetNodeHealth 获取节点健康状态
func (hc *HealthChecker) GetNodeHealth(nodeID string) (*NodeHealth, error) {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    node, exists := hc.nodes[nodeID]
    if !exists {
        return nil, fmt.Errorf("节点 %s 不存在", nodeID)
    }
    
    // 创建副本以避免并发修改
    nodeCopy := *node
    
    return &nodeCopy, nil
}

// GetAllNodesHealth 获取所有节点健康状态
func (hc *HealthChecker) GetAllNodesHealth() map[string]*NodeHealth {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    // 创建副本以避免并发修改
    result := make(map[string]*NodeHealth, len(hc.nodes))
    for id, node := range hc.nodes {
        nodeCopy := *node
        result[id] = &nodeCopy
    }
    
    return result
}

// SetNodeMaintenance 设置节点为维护状态
func (hc *HealthChecker) SetNodeMaintenance(nodeID string, maintenance bool) error {
    hc.mutex.Lock()
    defer hc.mutex.Unlock()
    
    node, exists := hc.nodes[nodeID]
    if !exists {
        return fmt.Errorf("节点 %s 不存在", nodeID)
    }
    
    oldState := node.State
    
    if maintenance {
        // 设置为维护状态
        if node.State != NodeStateMaintenance {
            node.State = NodeStateMaintenance
            
            // 发送状态变更事件
            go func() {
                hc.sendStateChangeEvent(nodeID, oldState, NodeStateMaintenance, "手动设置为维护状态")
            }()
            
            log.Printf("节点 %s 已设置为维护状态", nodeID)
        }
    } else {
        // 从维护状态恢复
        if node.State == NodeStateMaintenance {
            node.State = NodeStateUp
            
            // 发送状态变更事件
            go func() {
                hc.sendStateChangeEvent(nodeID, oldState, NodeStateUp, "手动从维护状态恢复")
            }()
            
            log.Printf("节点 %s 已从维护状态恢复", nodeID)
        }
    }
    
    return nil
}

// GetClusterHealth 获取集群整体健康状态
func (hc *HealthChecker) GetClusterHealth() (map[string]int, error) {
    hc.mutex.RLock()
    defer hc.mutex.RUnlock()
    
    // 统计各状态节点数量
    stats := map[string]int{
        string(NodeStateUp):          0,
        string(NodeStateDown):        0,
        string(NodeStateDegraded):    0,
        string(NodeStateMaintenance): 0,
        string(NodeStateUnknown):     0,
    }
    
    for _, node := range hc.nodes {
        stats[string(node.State)]++
    }
    
    return stats, nil
}