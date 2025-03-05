package transaction

import (
    "encoding/json"
    "fmt"
    "log"
    "strconv"
    "sync"
    "time"
)

// RecoveryManager 事务恢复管理器
type RecoveryManager struct {
    recoveryLog      *RecoveryLog
    tccCoordinator   *TCCCoordinator
    distCoordinator  *DistributedCoordinator
    mutex            sync.Mutex
    recoveryInterval time.Duration
}

// NewRecoveryManager 创建事务恢复管理器
func NewRecoveryManager(
    recoveryLog *RecoveryLog,
    tccCoordinator *TCCCoordinator,
    distCoordinator *DistributedCoordinator,
) *RecoveryManager {
    return &RecoveryManager{
        recoveryLog:      recoveryLog,
        tccCoordinator:   tccCoordinator,
        distCoordinator:  distCoordinator,
        recoveryInterval: 30 * time.Second, // 默认恢复检查间隔
    }
}

// LogTransactionState 记录事务状态
func (rm *RecoveryManager) LogTransactionState(txID string) error {
    rm.mutex.Lock()
    defer rm.mutex.Unlock()
    
    // 尝试获取TCC事务
    tccTx, err := rm.tccCoordinator.GetTransaction(txID)
    if err == nil {
        return rm.recoveryLog.LogTCCTransaction(tccTx)
    }
    
    // 尝试获取分布式事务
    distTx, err := rm.distCoordinator.GetTransaction(txID)
    if err == nil {
        return rm.recoveryLog.LogDistributedTransaction(distTx)
    }
    
    return fmt.Errorf("事务 %s 不存在", txID)
}

// CleanupTransactionLog 清理已完成事务的日志
func (rm *RecoveryManager) CleanupTransactionLog(txID string) error {
    return rm.recoveryLog.RemoveLog(txID)
}

// StartRecoveryProcess 启动恢复进程
func (rm *RecoveryManager) StartRecoveryProcess() {
    // 立即执行一次恢复
    if err := rm.RecoverPendingTransactions(); err != nil {
        log.Printf("恢复挂起事务失败: %v", err)
    }
    
    // 定期检查恢复
    go func() {
        ticker := time.NewTicker(rm.recoveryInterval)
        defer ticker.Stop()
        
        for range ticker.C {
            if err := rm.RecoverPendingTransactions(); err != nil {
                log.Printf("恢复挂起事务失败: %v", err)
            }
        }
    }()
    
    log.Println("事务恢复进程已启动")
}

// RecoverPendingTransactions 恢复所有挂起的事务
func (rm *RecoveryManager) RecoverPendingTransactions() error {
    rm.mutex.Lock()
    defer rm.mutex.Unlock()
    
    // 从日志中获取所有事务
    entries, err := rm.recoveryLog.RecoverTransactions()
    if err != nil {
        return fmt.Errorf("从日志恢复事务失败: %v", err)
    }
    
    log.Printf("发现 %d 个需要恢复的事务", len(entries))
    
    // 恢复每个事务
    for _, entry := range entries {
        // 检查事务是否已经太旧（超过24小时）
        if time.Since(entry.LastUpdate) > 24*time.Hour {
            log.Printf("事务 %s 已过期，将被清理", entry.TxID)
            rm.recoveryLog.RemoveLog(entry.TxID)
            continue
        }
        
        // 根据事务类型恢复
        switch entry.Type {
        case "tcc":
            if err := rm.recoverTCCTransaction(entry); err != nil {
                log.Printf("恢复TCC事务 %s 失败: %v", entry.TxID, err)
            }
        case "distributed":
            if err := rm.recoverDistributedTransaction(entry); err != nil {
                log.Printf("恢复分布式事务 %s 失败: %v", entry.TxID, err)
            }
        default:
            log.Printf("未知事务类型: %s", entry.Type)
        }
    }
    
    return nil
}

// recoverTCCTransaction 恢复TCC事务
func (rm *RecoveryManager) recoverTCCTransaction(entry TransactionLogEntry) error {
    // 检查事务是否已存在
    _, err := rm.tccCoordinator.GetTransaction(entry.TxID)
    if err == nil {
        // 事务已存在，直接恢复
        return rm.tccCoordinator.RecoverTransaction(entry.TxID)
    }
    
    // 事务不存在，需要重建
    log.Printf("重建TCC事务 %s", entry.TxID)
    
    // 解析参与者信息
    var participants []TCCParticipant
    if err := json.Unmarshal(entry.Participants, &participants); err != nil {
        return fmt.Errorf("解析参与者信息失败: %v", err)
    }
    
    // 创建新事务
    tx := &TCCTransaction{
        ID:           entry.TxID,
        State:        TCCState(entry.State),
        Participants: participants,
        StartTime:    entry.StartTime,
        Timeout:      30 * time.Second, // 默认超时
    }
    
    // 将事务添加到协调器
    rm.tccCoordinator.mutex.Lock()
    rm.tccCoordinator.transactions[entry.TxID] = tx
    rm.tccCoordinator.mutex.Unlock()
    
    // 恢复事务
    return rm.tccCoordinator.RecoverTransaction(entry.TxID)
}

// recoverDistributedTransaction 恢复分布式事务
func (rm *RecoveryManager) recoverDistributedTransaction(entry TransactionLogEntry) error {
    // 检查事务是否已存在
    _, err := rm.distCoordinator.GetTransaction(entry.TxID)
    if err == nil {
        // 事务已存在，直接恢复
        return rm.distCoordinator.RecoverTransaction(entry.TxID)
    }
    
    // 事务不存在，需要重建
    log.Printf("重建分布式事务 %s", entry.TxID)
    
    // 解析参与者信息
    var participants []string
    if err := json.Unmarshal(entry.Participants, &participants); err != nil {
        return fmt.Errorf("解析参与者信息失败: %v", err)
    }
    
    // 解析协议
    protocol, err := strconv.Atoi(entry.Protocol)
    if err != nil {
        return fmt.Errorf("解析协议失败: %v", err)
    }
    
    // 创建新事务
    tx := &DistributedTransaction{
        ID:           entry.TxID,
        Protocol:     TransactionProtocol(protocol),
        State:        TransactionState(entry.State),
        Participants: participants,
        StartTime:    entry.StartTime,
        Timeout:      30 * time.Second, // 默认超时
        Votes:        make(map[string]bool),
    }
    
    // 将事务添加到协调器
    rm.distCoordinator.mutex.Lock()
    rm.distCoordinator.transactions[entry.TxID] = tx
    rm.distCoordinator.mutex.Unlock()
    
    // 恢复事务
    return rm.distCoordinator.RecoverTransaction(entry.TxID)
}

// SetRecoveryInterval 设置恢复检查间隔
func (rm *RecoveryManager) SetRecoveryInterval(interval time.Duration) {
    rm.mutex.Lock()
    defer rm.mutex.Unlock()
    rm.recoveryInterval = interval
}