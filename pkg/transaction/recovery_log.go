package transaction

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "path/filepath"
    "sync"
    "time"
)

// RecoveryLog 事务恢复日志
type RecoveryLog struct {
    logDir      string
    mutex       sync.Mutex
    flushTicker *time.Ticker
}

// TransactionLogEntry 事务日志条目
type TransactionLogEntry struct {
    TxID        string          `json:"tx_id"`
    Type        string          `json:"type"`          // "distributed" 或 "tcc"
    Protocol    string          `json:"protocol"`      // 对于分布式事务
    State       string          `json:"state"`
    Participants json.RawMessage `json:"participants"` // 参与者信息
    StartTime   time.Time       `json:"start_time"`
    LastUpdate  time.Time       `json:"last_update"`
}

// NewRecoveryLog 创建事务恢复日志
func NewRecoveryLog(logDir string) (*RecoveryLog, error) {
    // 确保日志目录存在
    if err := os.MkdirAll(logDir, 0755); err != nil {
        return nil, fmt.Errorf("创建日志目录失败: %v", err)
    }
    
    rl := &RecoveryLog{
        logDir:      logDir,
        flushTicker: time.NewTicker(5 * time.Second), // 定期刷新日志
    }
    
    // 启动定期刷新
    go rl.flushRoutine()
    
    return rl, nil
}

// LogDistributedTransaction 记录分布式事务状态
func (rl *RecoveryLog) LogDistributedTransaction(tx *DistributedTransaction) error {
    rl.mutex.Lock()
    defer rl.mutex.Unlock()
    
    // 序列化参与者信息
    participants, err := json.Marshal(tx.Participants)
    if err != nil {
        return fmt.Errorf("序列化参与者信息失败: %v", err)
    }
    
    // 创建日志条目
    entry := TransactionLogEntry{
        TxID:        tx.ID,
        Type:        "distributed",
        Protocol:    fmt.Sprintf("%d", tx.Protocol),
        State:       string(tx.State),
        Participants: participants,
        StartTime:   tx.StartTime,
        LastUpdate:  time.Now(),
    }
    
    // 序列化日志条目
    data, err := json.Marshal(entry)
    if err != nil {
        return fmt.Errorf("序列化日志条目失败: %v", err)
    }
    
    // 写入日志文件
    logFile := filepath.Join(rl.logDir, fmt.Sprintf("tx_%s.json", tx.ID))
    if err := ioutil.WriteFile(logFile, data, 0644); err != nil {
        return fmt.Errorf("写入日志文件失败: %v", err)
    }
    
    return nil
}

// LogTCCTransaction 记录TCC事务状态
func (rl *RecoveryLog) LogTCCTransaction(tx *TCCTransaction) error {
    rl.mutex.Lock()
    defer rl.mutex.Unlock()
    
    // 序列化参与者信息
    participants, err := json.Marshal(tx.Participants)
    if err != nil {
        return fmt.Errorf("序列化参与者信息失败: %v", err)
    }
    
    // 创建日志条目
    entry := TransactionLogEntry{
        TxID:        tx.ID,
        Type:        "tcc",
        State:       string(tx.State),
        Participants: participants,
        StartTime:   tx.StartTime,
        LastUpdate:  time.Now(),
    }
    
    // 序列化日志条目
    data, err := json.Marshal(entry)
    if err != nil {
        return fmt.Errorf("序列化日志条目失败: %v", err)
    }
    
    // 写入日志文件
    logFile := filepath.Join(rl.logDir, fmt.Sprintf("tx_%s.json", tx.ID))
    if err := ioutil.WriteFile(logFile, data, 0644); err != nil {
        return fmt.Errorf("写入日志文件失败: %v", err)
    }
    
    return nil
}

// RemoveLog 移除事务日志
func (rl *RecoveryLog) RemoveLog(txID string) error {
    rl.mutex.Lock()
    defer rl.mutex.Unlock()
    
    logFile := filepath.Join(rl.logDir, fmt.Sprintf("tx_%s.json", txID))
    if err := os.Remove(logFile); err != nil && !os.IsNotExist(err) {
        return fmt.Errorf("移除日志文件失败: %v", err)
    }
    
    return nil
}

// RecoverTransactions 从日志恢复事务
func (rl *RecoveryLog) RecoverTransactions() ([]TransactionLogEntry, error) {
    rl.mutex.Lock()
    defer rl.mutex.Unlock()
    
    var entries []TransactionLogEntry
    
    // 读取日志目录中的所有文件
    files, err := ioutil.ReadDir(rl.logDir)
    if err != nil {
        return nil, fmt.Errorf("读取日志目录失败: %v", err)
    }
    
    for _, file := range files {
        if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
            continue
        }
        
        // 读取日志文件
        logFile := filepath.Join(rl.logDir, file.Name())
        data, err := ioutil.ReadFile(logFile)
        if err != nil {
            log.Printf("读取日志文件 %s 失败: %v", logFile, err)
            continue
        }
        
        // 解析日志条目
        var entry TransactionLogEntry
        if err := json.Unmarshal(data, &entry); err != nil {
            log.Printf("解析日志文件 %s 失败: %v", logFile, err)
            continue
        }
        
        entries = append(entries, entry)
    }
    
    return entries, nil
}

// flushRoutine 定期刷新日志
func (rl *RecoveryLog) flushRoutine() {
    for range rl.flushTicker.C {
        // 在实际实现中，可以在这里执行额外的刷新操作
        // 例如，确保所有日志文件都已写入磁盘
        log.Println("事务恢复日志定期刷新")
    }
}

// Close 关闭恢复日志
func (rl *RecoveryLog) Close() {
    rl.flushTicker.Stop()
}