package transaction

import (
	"sync"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/storage"
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewManager 创建一个新的事务管理器
func NewManager(db *storage.DB, defaultTimeout time.Duration) *Manager {
	// 创建结果缓存
	resultCache, _ := lru.New[string, []byte](10000)

	// 创建分片锁，提高并发性能
	shardCount := 32 // 使用32个分片锁
	shardMutexes := make([]sync.RWMutex, shardCount)

	m := &Manager{
		db:                 db,
		activeTransactions: make(map[string]*Transaction),
		defaultTimeout:     defaultTimeout,
		defaultIsolation:   RepeatableRead, // 默认使用可重复读隔离级别
		lockManager:        NewLockManager(defaultTimeout),
		cleanupTicker:      time.NewTicker(defaultTimeout / 2), // 定期清理的间隔为超时时间的一半
		statistics:         &ManagerStats{},
		shardCount:         shardCount,
		shardMutexes:       shardMutexes,
		deadlockDetector:   NewDeadlockDetector(),
		resultCache:        resultCache,
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
		readCache:      make(map[string][]byte),
		statistics:     &TxStats{},
		priority:       int(time.Now().UnixNano() % 100), // 简单的优先级分配
		dependencies:   make(map[string]struct{}),
	}

	m.activeTransactions[id] = tx
	
	// 更新统计信息
	m.statistics.mutex.Lock()
	m.statistics.TxStarted++
	m.statistics.mutex.Unlock()
	
	return tx, nil
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
				// 再次检