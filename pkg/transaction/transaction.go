package transaction

import (
	"errors"
	"fmt"
	"sync"
	"time"

	// 使用模块路径
	"github.com/suonanjiexi/go-data/pkg/storage"
	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义错误类型
var (
	ErrTxNotActive = errors.New("transaction not active")
	ErrTxTimeout   = errors.New("transaction timeout")
	ErrTxConflict  = errors.New("transaction conflict")
	ErrDeadlock    = errors.New("transaction deadlock detected")
)

// 事务状态
const (
	TxStatusActive = iota
	TxStatusCommitted
	TxStatusAborted
)

// 事务隔离级别
const (
	ReadUncommitted = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// Transaction 表示一个数据库事务
type Transaction struct {
	id             string              // 事务ID
	status         int                 // 事务状态
	startTime      time.Time           // 事务开始时间
	timeout        time.Duration       // 事务超时时间
	db             *storage.DB         // 底层数据库
	writeSet       map[string][]byte   // 写集合
	readSet        map[string]struct{} // 读集合
	mutex          sync.RWMutex        // 读写锁
	isolationLevel int                 // 事务隔离级别
	readCache      map[string][]byte   // 读缓存，避免重复读取相同的键
	statistics     *TxStats            // 事务统计信息
	priority       int                 // 事务优先级，用于死锁预防
	dependencies   map[string]struct{} // 事务依赖关系，用于死锁检测
}

// TxStats 事务统计信息
type TxStats struct {
	ReadCount      int           // 读操作次数
	WriteCount     int           // 写操作次数
	Conflicts      int           // 冲突次数
	Retries        int           // 重试次数
	TotalReadTime  time.Duration // 总读取时间
	TotalWriteTime time.Duration // 总写入时间
	mutex          sync.Mutex    // 统计信息锁
}

// Manager 事务管理器
type Manager struct {
	db                 *storage.DB
	activeTransactions map[string]*Transaction
	mutex              sync.RWMutex
	defaultTimeout     time.Duration
	defaultIsolation   int
	lockManager        *LockManager
	cleanupTicker      *time.Ticker               // 用于定期清理超时事务
	statistics         *ManagerStats              // 事务管理器统计信息
	shardCount         int                        // 分片数量，用于细粒度锁控制
	shardMutexes       []sync.RWMutex             // 分片锁数组
	deadlockDetector   *DeadlockDetector          // 死锁检测器
	resultCache        *lru.Cache[string, []byte] // 查询结果缓存
}

// ManagerStats 事务管理器统计信息
type ManagerStats struct {
	TxStarted     int64         // 启动的事务数
	TxCommitted   int64         // 提交的事务数
	TxAborted     int64         // 中止的事务数
	TxTimeout     int64         // 超时的事务数
	Deadlocks     int64         // 检测到的死锁数
	AvgTxDuration time.Duration // 平均事务持续时间
	mutex         sync.Mutex    // 统计信息锁
}

// DeadlockDetector 死锁检测器
type DeadlockDetector struct {
	waitForGraph map[string]map[string]struct{} // 等待图：txID -> 等待的事务ID集合
	mutex        sync.Mutex                     // 互斥锁
}

// LockManager 在lock.go中实现

// NewManager 创建一个新的事务管理器
func NewManager(db *storage.DB, defaultTimeout time.Duration) *Manager {
	m := &Manager{
		db:                 db,
		activeTransactions: make(map[string]*Transaction),
		defaultTimeout:     defaultTimeout,
		defaultIsolation:   RepeatableRead, // 默认使用可重复读隔离级别
		lockManager:        NewLockManager(defaultTimeout),
		cleanupTicker:      time.NewTicker(defaultTimeout / 2), // 定期清理的间隔为超时时间的一半
	}

	// 启动后台清理goroutine
	go m.cleanupRoutine()

	return m
}

// Begin 开始一个新事务
func (m *Manager) Begin() (*Transaction, error) {
	return m.BeginWithIsolation(m.defaultIsolation)
}

// BeginWithIsolation 以指定隔离级别开始一个新事务
func (m *Manager) BeginWithIsolation(isolationLevel int) (*Transaction, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 生成唯一事务ID
	id := generateTxID()

	tx := &Transaction{
		id:             id,
		status:         TxStatusActive,
		startTime:      time.Now(),
		timeout:        m.defaultTimeout,
		db:             m.db,
		writeSet:       make(map[string][]byte),
		readSet:        make(map[string]struct{}),
		isolationLevel: isolationLevel,
		mutex:          sync.RWMutex{},
	}

	m.activeTransactions[id] = tx
	return tx, nil
}

// Commit 提交事务
func (tx *Transaction) Commit() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将写集合应用到数据库
	for key, value := range tx.writeSet {
		if value == nil {
			// 删除操作
			if err := tx.db.Delete(key); err != nil {
				tx.status = TxStatusAborted
				return err
			}
		} else {
			// 写入操作
			if err := tx.db.Put(key, value); err != nil {
				tx.status = TxStatusAborted
				return err
			}
		}
	}

	tx.status = TxStatusCommitted
	return nil
}

// Rollback 回滚事务
func (tx *Transaction) Rollback() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	tx.status = TxStatusAborted
	return nil
}

// Get 从事务中读取数据
func (tx *Transaction) Get(key string) ([]byte, error) {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return nil, ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return nil, ErrTxTimeout
	}

	// 首先检查写集合
	if value, ok := tx.writeSet[key]; ok {
		if value == nil { // 键已被删除
			return nil, storage.ErrKeyNotFound
		}
		return value, nil
	}

	// 然后从数据库读取
	value, err := tx.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 添加到读集合
	tx.readSet[key] = struct{}{}
	return value, nil
}

// Put 在事务中写入数据
func (tx *Transaction) Put(key string, value []byte) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将数据添加到写集合
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	tx.writeSet[key] = valueCopy
	return nil
}

// Delete 在事务中删除数据
func (tx *Transaction) Delete(key string) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将空值添加到写集合，表示删除
	tx.writeSet[key] = nil
	return nil
}

// 生成唯一事务ID
func generateTxID() string {
	// 使用时间戳和随机数生成更唯一的ID
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix()%1000)
}

// Status 返回事务的当前状态
func (tx *Transaction) Status() int {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	// 只读检查事务状态
	status := tx.status
	if status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
		// 如果检测到超时，需要升级为写锁来修改状态
		tx.mutex.RUnlock()
		tx.mutex.Lock()

		// 重新检查状态，因为可能在获取写锁期间已被其他goroutine修改
		if tx.status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
			tx.status = TxStatusAborted
		}

		status = tx.status
		tx.mutex.Unlock()
		return status
	}

	return status
}

// cleanupRoutine 定期清理超时的事务
func (m *Manager) cleanupRoutine() {
	for range m.cleanupTicker.C {
		// 使用读锁获取活跃事务列表，减少锁竞争
		m.mutex.RLock()
		activeIDs := make([]string, 0, len(m.activeTransactions))
		activeTxs := make([]*Transaction, 0, len(m.activeTransactions))

		for id, tx := range m.activeTransactions {
			activeIDs = append(activeIDs, id)
			activeTxs = append(activeTxs, tx)
		}
		m.mutex.RUnlock()

		// 检查每个事务，不需要持有全局锁
		toRemove := make([]string, 0)

		for i, id := range activeIDs {
			tx := activeTxs[i]
			tx.mutex.RLock()
			isTimeout := tx.status != TxStatusActive || time.Since(tx.startTime) > tx.timeout
			tx.mutex.RUnlock()

			if isTimeout {
				// 获取写锁来修改事务状态
				tx.mutex.Lock()
				// 再次检查状态，因为可能在获取写锁期间已被其他goroutine修改
				if tx.status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
					// 将事务标记为中止
					tx.status = TxStatusAborted
				}

				// 获取需要释放的锁的键
				writeKeys := make([]string, 0, len(tx.writeSet))
				readKeys := make([]string, 0, len(tx.readSet))

				for key := range tx.writeSet {
					writeKeys = append(writeKeys, key)
				}
				for key := range tx.readSet {
					readKeys = append(readKeys, key)
				}
				tx.mutex.Unlock()

				// 释放事务持有的所有锁
				for _, key := range writeKeys {
					m.lockManager.ReleaseLock(id, key)
				}
				for _, key := range readKeys {
					m.lockManager.ReleaseLock(id, key)
				}

				// 标记要移除的事务
				toRemove = append(toRemove, id)
			}
		}

		// 只在需要移除事务时获取写锁
		if len(toRemove) > 0 {
			m.mutex.Lock()
			for _, id := range toRemove {
				delete(m.activeTransactions, id)
			}
			m.mutex.Unlock()
		}

		// 清理超时的锁
		m.lockManager.CleanupTimeouts()
	}
}
