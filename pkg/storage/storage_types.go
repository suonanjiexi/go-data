package storage

import (
	"bytes"
	"errors"
	"sync"
	"time"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义错误类型
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBNotOpen   = errors.New("database not open")
)

// DB 表示一个简单的键值存储数据库
type DB struct {
	path          string                     // 数据库文件路径
	isOpen        bool                       // 数据库是否打开
	data          map[string][]byte          // 内存中的数据
	cache         *lru.Cache[string, []byte] // LRU缓存
	mutex         sync.RWMutex               // 读写锁，保证并发安全
	persistent    bool                       // 是否持久化存储
	counters      map[string]int             // 自增ID计数器
	counterMutex  sync.RWMutex               // 计数器的互斥锁
	charset       string                     // 字符集编码，默认为utf8mb4
	indexManager  *IndexManager              // 索引管理器
	batchSize     int                        // 批量写入大小
	batchBuffer   []writeOp                  // 批量写入缓冲区
	batchMutex    sync.Mutex                 // 批量写入互斥锁
	flushTicker   *time.Ticker               // 定期刷新计时器
	flushDone     chan struct{}              // 用于停止刷新goroutine的通道
	preReadSize   int                        // 预读取大小
	preReadBuffer *sync.Map                  // 预读取缓冲区
	writeBuffer   *bytes.Buffer              // 写入缓冲区
	compression   bool                       // 是否启用压缩
	memoryPool    sync.Pool                  // 内存池
	shardCount    int                        // 分片数量，用于细粒度锁控制
	shardMutexes  []sync.RWMutex             // 分片锁数组
	resultCache   *lru.Cache[string, []byte] // 查询结果缓存
	
	// 统计信息字段
	cacheHits      int64         // 缓存命中次数
	cacheMisses    int64         // 缓存未命中次数
	getOps         int64         // Get操作次数
	putOps         int64         // Put操作次数
	deleteOps      int64         // Delete操作次数
	avgGetTime     time.Duration // 平均Get操作时间
	avgPutTime     time.Duration // 平均Put操作时间
	lastFlushTime  time.Time     // 最后一次刷新时间
	lastBackupTime time.Time     // 最后一次备份时间
	startTime      time.Time     // 数据库启动时间
}

// writeOp 表示一个写操作
type writeOp struct {
	key   string
	value []byte
}

// DBStats 数据库统计信息
type DBStats struct {
	KeyCount        int           // 键值对数量
	TotalDataSize   int64         // 总数据大小（字节）
	CacheHitRate    float64       // 缓存命中率
	CacheHits       int64         // 缓存命中次数
	CacheMisses     int64         // 缓存未命中次数
	GetOps          int64         // Get操作次数
	PutOps          int64         // Put操作次数
	DeleteOps       int64         // Delete操作次数
	AvgGetTime      time.Duration // 平均Get操作时间
	AvgPutTime      time.Duration // 平均Put操作时间
	IndexCount      int           // 索引数量
	LastFlushTime   time.Time     // 最后一次刷新时间
	LastBackupTime  time.Time     // 最后一次备份时间
	StartTime       time.Time     // 数据库启动时间
	Uptime          time.Duration // 运行时间
}