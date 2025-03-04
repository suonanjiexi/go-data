package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// TransactionState 定义事务状态
type TransactionState int

const (
	TxUnknown   TransactionState = iota
	TxPrepared                   // 准备阶段
	TxCommitted                  // 已提交
	TxAborted                    // 已回滚
)

// LogEntry 表示一个日志条目
type LogEntry struct {
	Term    int64       // 任期号
	Index   int64       // 日志索引
	Command interface{} // 命令内容
}

// TransactionInfo 分布式事务信息
type TransactionInfo struct {
	ID        string           // 事务ID
	State     TransactionState // 事务状态
	StartTime time.Time        // 开始时间
	Timeout   time.Duration    // 超时时间
	Nodes     []string         // 参与节点列表
	LogEntry  *LogEntry        // 关联的日志条目
}

// TransactionCoordinator 事务协调器
type TransactionCoordinator struct {
	mutex        sync.RWMutex
	transactions map[string]*TransactionInfo // 活跃事务列表
	node         *Node                       // 所属节点

	// Raft日志相关
	logs        []*LogEntry      // 日志条目
	commitIndex int64            // 已提交的最高日志索引
	lastApplied int64            // 已应用到状态机的最高日志索引
	nextIndex   map[string]int64 // 每个节点的下一个日志索引
	matchIndex  map[string]int64 // 每个节点已复制的最高日志索引
}

// NewTransactionCoordinator 创建事务协调器
func NewTransactionCoordinator(node *Node) *TransactionCoordinator {
	return &TransactionCoordinator{
		transactions: make(map[string]*TransactionInfo),
		node:         node,
		logs:         make([]*LogEntry, 0),
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make(map[string]int64),
		matchIndex:   make(map[string]int64),
	}
}

// Begin 开始新的分布式事务
func (tc *TransactionCoordinator) Begin(nodes []string, timeout time.Duration) (string, error) {
	// 只有Leader节点可以创建新事务
	if tc.node.raftState != Leader {
		return "", errors.New("not leader")
	}

	txID := fmt.Sprintf("tx-%d", time.Now().UnixNano())
	txInfo := &TransactionInfo{
		ID:        txID,
		State:     TxUnknown,
		StartTime: time.Now(),
		Timeout:   timeout,
		Nodes:     nodes,
	}

	// 创建日志条目
	entry := &LogEntry{
		Term:    tc.node.currentTerm,
		Index:   int64(len(tc.logs) + 1),
		Command: txInfo,
	}
	txInfo.LogEntry = entry

	// 追加日志
	tc.logs = append(tc.logs, entry)

	// 复制日志到其他节点
	tc.replicateLog(entry)

	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.transactions[txID] = txInfo

	return txID, nil
}

// replicateLog 复制日志到其他节点
func (tc *TransactionCoordinator) replicateLog(entry *LogEntry) {
	for _, peer := range tc.node.GetPeers() {
		go func(p *NodeInfo) {
			for {
				// 获取该节点的下一个日志索引
				nextIdx := tc.nextIndex[p.ID]

				// 发送AppendEntries RPC
				success := tc.sendAppendEntries(p.ID, nextIdx, entry)

				if success {
					// 更新matchIndex和nextIndex
					tc.matchIndex[p.ID] = entry.Index
					tc.nextIndex[p.ID] = entry.Index + 1
					break
				} else {
					// 如果失败，减少nextIndex并重试
					tc.nextIndex[p.ID]--
				}
			}
		}(peer)
	}
}

// sendAppendEntries 发送追加日志RPC
func (tc *TransactionCoordinator) sendAppendEntries(nodeID string, nextIdx int64, entry *LogEntry) bool {
	// TODO: 实现发送AppendEntries RPC的逻辑
	return true
}

// Prepare 准备阶段
func (tc *TransactionCoordinator) Prepare(txID string) error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	txInfo, exists := tc.transactions[txID]
	if !exists {
		return errors.New("transaction not found")
	}

	// 检查事务是否超时
	if time.Since(txInfo.StartTime) > txInfo.Timeout {
		return errors.New("transaction timeout")
	}

	// 创建准备阶段的日志条目
	entry := &LogEntry{
		Term:  tc.node.currentTerm,
		Index: int64(len(tc.logs) + 1),
		Command: map[string]interface{}{
			"type": "prepare",
			"txID": txID,
		},
	}

	// 追加并复制日志
	tc.logs = append(tc.logs, entry)
	tc.replicateLog(entry)

	txInfo.State = TxPrepared
	return nil
}

// Commit 提交事务
func (tc *TransactionCoordinator) Commit(txID string) error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	txInfo, exists := tc.transactions[txID]
	if !exists {
		return errors.New("transaction not found")
	}

	// 检查事务状态
	if txInfo.State != TxPrepared {
		return errors.New("transaction not in prepared state")
	}

	// 创建提交阶段的日志条目
	entry := &LogEntry{
		Term:  tc.node.currentTerm,
		Index: int64(len(tc.logs) + 1),
		Command: map[string]interface{}{
			"type": "commit",
			"txID": txID,
		},
	}

	// 追加并复制日志
	tc.logs = append(tc.logs, entry)
	tc.replicateLog(entry)

	txInfo.State = TxCommitted
	return nil
}

// Abort 回滚事务
func (tc *TransactionCoordinator) Abort(txID string) error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	txInfo, exists := tc.transactions[txID]
	if !exists {
		return errors.New("transaction not found")
	}

	// 创建回滚阶段的日志条目
	entry := &LogEntry{
		Term:  tc.node.currentTerm,
		Index: int64(len(tc.logs) + 1),
		Command: map[string]interface{}{
			"type": "abort",
			"txID": txID,
		},
	}

	// 追加并复制日志
	tc.logs = append(tc.logs, entry)
	tc.replicateLog(entry)

	txInfo.State = TxAborted
	return nil
}

// applyLog 应用日志到状态机
func (tc *TransactionCoordinator) applyLog(entry *LogEntry) error {
	// 根据日志类型执行相应的操作
	switch cmd := entry.Command.(type) {
	case *TransactionInfo:
		// 处理新事务
		tc.transactions[cmd.ID] = cmd
	case map[string]interface{}:
		// 处理prepare/commit/abort命令
		txID := cmd["txID"].(string)
		if tx, exists := tc.transactions[txID]; exists {
			switch cmd["type"] {
			case "prepare":
				tx.State = TxPrepared
			case "commit":
				tx.State = TxCommitted
			case "abort":
				tx.State = TxAborted
			}
		}
	}

	// 更新lastApplied
	tc.lastApplied = entry.Index
	return nil
}

// Cleanup 清理已完成的事务
func (tc *TransactionCoordinator) Cleanup() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	for id, tx := range tc.transactions {
		if tx.State == TxCommitted || tx.State == TxAborted {
			delete(tc.transactions, id)
		}
	}
}
