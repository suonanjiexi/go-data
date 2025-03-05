package transaction

import (
	"errors"
	"sync"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/storage"
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