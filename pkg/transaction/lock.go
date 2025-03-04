package transaction

import (
	"fmt"
	"sync"
	"time"
)

// LockType 锁类型
type LockType int

const (
	ReadLock LockType = iota
	WriteLock
)

// LockRequest 锁请求
type LockRequest struct {
	TxID     string    // 事务ID
	Key      string    // 请求锁定的键
	Type     LockType  // 锁类型
	Deadline time.Time // 请求超时时间
}

// LockEntry 锁条目
type LockEntry struct {
	Type      LockType  // 锁类型
	Holder    string    // 持有锁的事务ID
	WaitQueue []string  // 等待队列（事务ID列表）
	Timestamp time.Time // 获取锁的时间戳
}

// LockManager 增强的锁管理器
type LockManager struct {
	locks      map[string]*LockEntry      // 键 -> 锁条目
	waitForMap map[string]map[string]bool // 事务等待图（用于死锁检测）
	mutex      sync.RWMutex
	timeout    time.Duration // 锁超时时间
}

// NewLockManager 创建一个新的锁管理器
func NewLockManager(timeout time.Duration) *LockManager {
	return &LockManager{
		locks:      make(map[string]*LockEntry),
		waitForMap: make(map[string]map[string]bool),
		timeout:    timeout,
	}
}

// AcquireLock 获取锁
func (lm *LockManager) AcquireLock(req *LockRequest) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// 检查请求是否已超时
	if time.Now().After(req.Deadline) {
		return ErrTxTimeout
	}

	// 获取或创建锁条目
	entry, exists := lm.locks[req.Key]
	if !exists {
		// 如果锁不存在，创建新的锁条目
		entry = &LockEntry{
			Type:      req.Type,
			Holder:    req.TxID,
			WaitQueue: make([]string, 0),
			Timestamp: time.Now(),
		}
		lm.locks[req.Key] = entry
		return nil
	}

	// 检查是否可以获取锁
	if entry.Holder == req.TxID {
		// 已经持有锁
		if entry.Type == WriteLock || req.Type == ReadLock {
			return nil
		}
		// 升级到写锁
		if len(entry.WaitQueue) == 0 {
			entry.Type = WriteLock
			return nil
		}
		return ErrTxConflict
	}

	// 检查锁兼容性
	if entry.Type == ReadLock && req.Type == ReadLock && len(entry.WaitQueue) == 0 {
		// 可以共享读锁
		entry.Holder = req.TxID
		return nil
	}

	// 添加到等待队列
	entry.WaitQueue = append(entry.WaitQueue, req.TxID)

	// 更新等待图
	if _, exists := lm.waitForMap[req.TxID]; !exists {
		lm.waitForMap[req.TxID] = make(map[string]bool)
	}
	lm.waitForMap[req.TxID][entry.Holder] = true

	// 检查死锁
	if lm.hasDeadlock(req.TxID, make(map[string]bool)) {
		// 从等待队列中移除
		for i, txID := range entry.WaitQueue {
			if txID == req.TxID {
				entry.WaitQueue = append(entry.WaitQueue[:i], entry.WaitQueue[i+1:]...)
				break
			}
		}
		// 清理等待图
		delete(lm.waitForMap[req.TxID], entry.Holder)
		if len(lm.waitForMap[req.TxID]) == 0 {
			delete(lm.waitForMap, req.TxID)
		}
		return ErrDeadlock
	}

	return ErrTxConflict
}

// ReleaseLock 释放锁
func (lm *LockManager) ReleaseLock(txID string, key string) error {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	entry, exists := lm.locks[key]
	if !exists {
		return nil
	}

	// 检查是否是锁持有者
	if entry.Holder != txID {
		return fmt.Errorf("transaction %s does not hold lock for key %s", txID, key)
	}

	// 从等待图中移除
	for waitingTx := range lm.waitForMap {
		delete(lm.waitForMap[waitingTx], txID)
		if len(lm.waitForMap[waitingTx]) == 0 {
			delete(lm.waitForMap, waitingTx)
		}
	}

	// 如果等待队列为空，删除锁
	if len(entry.WaitQueue) == 0 {
		delete(lm.locks, key)
		return nil
	}

	// 将锁授予等待队列中的下一个事务
	nextTxID := entry.WaitQueue[0]
	entry.WaitQueue = entry.WaitQueue[1:]
	entry.Holder = nextTxID
	entry.Timestamp = time.Now()

	return nil
}

// hasDeadlock 检查是否存在死锁（使用DFS检测环）
func (lm *LockManager) hasDeadlock(txID string, visited map[string]bool) bool {
	if visited[txID] {
		return true
	}

	visited[txID] = true
	for waitFor := range lm.waitForMap[txID] {
		if lm.hasDeadlock(waitFor, visited) {
			return true
		}
	}
	delete(visited, txID)

	return false
}

// CleanupTimeouts 清理超时的锁
func (lm *LockManager) CleanupTimeouts() {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	now := time.Now()
	for key, entry := range lm.locks {
		if now.Sub(entry.Timestamp) > lm.timeout {
			// 清理超时的锁
			delete(lm.locks, key)

			// 清理相关的等待图
			for waitingTx := range lm.waitForMap {
				delete(lm.waitForMap[waitingTx], entry.Holder)
				if len(lm.waitForMap[waitingTx]) == 0 {
					delete(lm.waitForMap, waitingTx)
				}
			}
		}
	}
}
