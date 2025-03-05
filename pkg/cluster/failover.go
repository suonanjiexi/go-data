package cluster

import (
    "fmt"
    "log"
    "sync"
    "time"
)

// FailoverManager 管理节点故障转移
type FailoverManager struct {
    clusterManager *ClusterManager
    mutex          sync.RWMutex
    inProgress     map[string]*FailoverOperation
    history        []*FailoverOperation
    maxHistory     int
    config         *FailoverConfig
}

// FailoverOperation 表示一次故障转移操作
type FailoverOperation struct {
    ID            string
    FailedNodeID  string
    StartTime     time.Time
    EndTime       time.Time
    Status        FailoverStatus
    Replacements  map[string]string // 分片ID -> 新节点ID
    Error         string
}

// FailoverStatus 故障转移状态
type FailoverStatus string

const (
    FailoverPending   FailoverStatus = "pending"
    FailoverInProgress FailoverStatus = "in_progress"
    FailoverCompleted FailoverStatus = "completed"
    FailoverFailed    FailoverStatus = "failed"
)

// FailoverConfig 故障转移配置
type FailoverConfig struct {
    Enabled             bool
    AutomaticFailover   bool
    FailoverTimeout     time.Duration
    MaxConcurrentFailovers int
    MinReplicaCount     int
}

// NewFailoverManager 创建故障转移管理器
func NewFailoverManager(cm *ClusterManager, config *FailoverConfig) *FailoverManager {
    if config == nil {
        config = &FailoverConfig{
            Enabled:             true,
            AutomaticFailover:   true,
            FailoverTimeout:     2 * time.Minute,
            MaxConcurrentFailovers: 3,
            MinReplicaCount:     1,
        }
    }
    
    return &FailoverManager{
        clusterManager: cm,
        inProgress:     make(map[string]*FailoverOperation),
        history:        make([]*FailoverOperation, 0),
        maxHistory:     100,
        config:         config,
    }
}

// HandleNodeFailure 处理节点故障
func (fm *FailoverManager) HandleNodeFailure(nodeID string) error {
    if !fm.config.Enabled {
        return fmt.Errorf("故障转移功能已禁用")
    }
    
    fm.mutex.Lock()
    
    // 检查是否已有该节点的故障转移操作
    if _, exists := fm.inProgress[nodeID]; exists {
        fm.mutex.Unlock()
        return fmt.Errorf("节点 %s 的故障转移操作已在进行中", nodeID)
    }
    
    // 检查并发故障转移数量
    if len(fm.inProgress) >= fm.config.MaxConcurrentFailovers {
        fm.mutex.Unlock()
        return fmt.Errorf("已达到最大并发故障转移数量 %d", fm.config.MaxConcurrentFailovers)
    }
    
    // 创建新的故障转移操作
    operation := &FailoverOperation{
        ID:           fmt.Sprintf("failover-%s-%d", nodeID, time.Now().Unix()),
        FailedNodeID: nodeID,
        StartTime:    time.Now(),
        Status:       FailoverPending,
        Replacements: make(map[string]string),
    }
    
    fm.inProgress[nodeID] = operation
    fm.mutex.Unlock()
    
    // 异步执行故障转移
    go fm.executeFailover(operation)
    
    return nil
}

// executeFailover 执行故障转移
func (fm *FailoverManager) executeFailover(op *FailoverOperation) {
    log.Printf("开始节点 %s 的故障转移操作 %s", op.FailedNodeID, op.ID)
    
    // 更新状态为进行中
    fm.mutex.Lock()
    op.Status = FailoverInProgress
    fm.mutex.Unlock()
    
    // 获取故障节点的分片
    shards, err := fm.clusterManager.GetNodeShards(op.FailedNodeID)
    if err != nil {
        fm.completeFailover(op, err)
        return
    }
    
    // 为每个分片选择新节点
    for _, shard := range shards {
        // 获取可用节点
        availableNodes, err := fm.clusterManager.GetAvailableNodes()
        if err != nil {
            fm.completeFailover(op, err)
            return
        }
        
        // 选择负载最低的节点
        newNodeID, err := fm.selectBestReplacement(availableNodes, op.FailedNodeID)
        if err != nil {
            fm.completeFailover(op, err)
            return
        }
        
        // 迁移分片
        if err := fm.clusterManager.MigrateShard(shard.ID, op.FailedNodeID, newNodeID); err != nil {
            fm.completeFailover(op, err)
            return
        }
        
        // 记录替换信息
        op.Replacements[shard.ID] = newNodeID
    }
    
    // 完成故障转移
    fm.completeFailover(op, nil)
}

// selectBestReplacement 选择最佳替代节点
func (fm *FailoverManager) selectBestReplacement(nodes []*NodeInfo, excludeNodeID string) (string, error) {
    if len(nodes) == 0 {
        return "", fmt.Errorf("没有可用节点进行故障转移")
    }
    
    var bestNode *NodeInfo
    var minLoad float64 = -1
    
    for _, node := range nodes {
        // 排除故障节点
        if node.ID == excludeNodeID {
            continue
        }
        
        // 排除非活跃节点
        if node.State != NodeStateUp {
            continue
        }
        
        // 计算节点负载
        load := node.CalculateLoad()
        
        // 选择负载最低的节点
        if minLoad == -1 || load < minLoad {
            minLoad = load
            bestNode = node
        }
    }
    
    if bestNode == nil {
        return "", fmt.Errorf("没有合适的节点进行故障转移")
    }
    
    return bestNode.ID, nil
}

// completeFailover 完成故障转移操作
func (fm *FailoverManager) completeFailover(op *FailoverOperation, err error) {
    fm.mutex.Lock()
    defer fm.mutex.Unlock()
    
    op.EndTime = time.Now()
    
    if err != nil {
        op.Status = FailoverFailed
        op.Error = err.Error()
        log.Printf("节点 %s 的故障转移操作 %s 失败: %v", op.FailedNodeID, op.ID, err)
    } else {
        op.Status = FailoverCompleted
        log.Printf("节点 %s 的故障转移操作 %s 成功完成", op.FailedNodeID, op.ID)
    }
    
    // 从进行中列表移除
    delete(fm.inProgress, op.FailedNodeID)
    
    // 添加到历史记录
    fm.history = append(fm.history, op)
    
    // 限制历史记录数量
    if len(fm.history) > fm.maxHistory {
        fm.history = fm.history[len(fm.history)-fm.maxHistory:]
    }
    
    // 更新集群状态
    fm.clusterManager.UpdateClusterState()
}

// GetFailoverHistory 获取故障转移历史记录
func (fm *FailoverManager) GetFailoverHistory() []*FailoverOperation {
    fm.mutex.RLock()
    defer fm.mutex.RUnlock()
    
    // 创建副本以避免并发修改
    history := make([]*FailoverOperation, len(fm.history))
    copy(history, fm.history)
    
    return history
}

// GetInProgressFailovers 获取正在进行的故障转移操作
func (fm *FailoverManager) GetInProgressFailovers() []*FailoverOperation {
    fm.mutex.RLock()
    defer fm.mutex.RUnlock()
    
    operations := make([]*FailoverOperation, 0, len(fm.inProgress))
    for _, op := range fm.inProgress {
        operations = append(operations, op)
    }
    
    return operations
}

// CancelFailover 取消故障转移操作
func (fm *FailoverManager) CancelFailover(nodeID string) error {
    fm.mutex.Lock()
    defer fm.mutex.Unlock()
    
    op, exists := fm.inProgress[nodeID]
    if !exists {
        return fmt.Errorf("节点 %s 没有正在进行的故障转移操作", nodeID)
    }
    
    // 只能取消待处理的操作
    if op.Status != FailoverPending {
        return fmt.Errorf("无法取消状态为 %s 的故障转移操作", op.Status)
    }
    
    // 更新状态
    op.Status = FailoverFailed
    op.EndTime = time.Now()
    op.Error = "手动取消"
    
    // 从进行中列表移除
    delete(fm.inProgress, nodeID)
    
    // 添加到历史记录
    fm.history = append(fm.history, op)
    
    log.Printf("节点 %s 的故障转移操作 %s 已取消", nodeID, op.ID)
    
    return nil
}

// HandleNodeRecovery 处理节点恢复
func (fm *FailoverManager) HandleNodeRecovery(nodeID string) error {
    log.Printf("节点 %s 已恢复，开始重新平衡分片", nodeID)
    
    // 获取集群中的所有分片
    shards, err := fm.clusterManager.GetAllShards()
    if err != nil {
        return err
    }
    
    // 查找最近从该节点迁移走的分片
    recentFailovers := fm.getRecentFailoversForNode(nodeID)
    
    // 如果有最近的故障转移记录，尝试将分片迁回
    if len(recentFailovers) > 0 {
        for _, op := range recentFailovers {
            for shardID, newNodeID := range op.Replacements {
                // 检查分片是否仍然存在
                shardExists := false
                for _, shard := range shards {
                    if shard.ID == shardID {
                        shardExists = true
                        break
                    }
                }
                
                if shardExists {
                    // 尝试将分片迁回原节点
                    if err := fm.clusterManager.MigrateShard(shardID, newNodeID, nodeID); err != nil {
                        log.Printf("将分片 %s 迁回节点 %s 失败: %v", shardID, nodeID, err)
                    } else {
                        log.Printf("成功将分片 %s 迁回节点 %s", shardID, nodeID)
                    }
                }
            }
        }
    } else {
        // 如果没有最近的故障转移记录，执行负载均衡
        if err := fm.clusterManager.RebalanceShards(); err != nil {
            return fmt.Errorf("重新平衡分片失败: %v", err)
        }
    }
    
    return nil
}

// getRecentFailoversForNode 获取节点最近的故障转移记录
func (fm *FailoverManager) getRecentFailoversForNode(nodeID string) []*FailoverOperation {
    fm.mutex.RLock()
    defer fm.mutex.RUnlock()
    
    // 查找最近24小时内该节点的故障转移记录
    cutoff := time.Now().Add(-24 * time.Hour)
    var operations []*FailoverOperation
    
    for i := len(fm.history) - 1; i >= 0; i-- {
        op := fm.history[i]
        if op.FailedNodeID == nodeID && op.Status == FailoverCompleted && op.EndTime.After(cutoff) {
            operations = append(operations, op)
        }
    }
    
    return operations
}

// MonitorFailoverOperations 监控故障转移操作超时
func (fm *FailoverManager) MonitorFailoverOperations() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        fm.checkFailoverTimeouts()
    }
}

// checkFailoverTimeouts 检查故障转移操作是否超时
func (fm *FailoverManager) checkFailoverTimeouts() {
    fm.mutex.Lock()
    defer fm.mutex.Unlock()
    
    now := time.Now()
    
    for nodeID, op := range fm.inProgress {
        // 检查是否超时
        if op.Status == FailoverInProgress && now.Sub(op.StartTime) > fm.config.FailoverTimeout {
            // 标记为失败
            op.Status = FailoverFailed
            op.EndTime = now
            op.Error = "操作超时"
            
            // 从进行中列表移除
            delete(fm.inProgress, nodeID)
            
            // 添加到历史记录
            fm.history = append(fm.history, op)
            
            log.Printf("节点 %s 的故障转移操作 %s 因超时而失败", nodeID, op.ID)
        }
    }
}