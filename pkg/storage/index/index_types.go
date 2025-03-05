package storage

import (
	"errors"
	"sync"
	"time"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义索引类型
const (
	BPlusTreeIndex = iota // B+树索引
	HashIndex             // 哈希索引
)

// 定义索引错误
var (
	ErrIndexExists      = errors.New("index already exists")
	ErrIndexNotFound    = errors.New("index not found")
	ErrInvalidIndex     = errors.New("invalid index configuration")
	ErrInvalidIndexType = errors.New("invalid index type")
)

// IndexConfig 表示索引配置
type IndexConfig struct {
	Name    string   // 索引名称
	Table   string   // 表名
	Columns []string // 索引列
	Type    int      // 索引类型
	Unique  bool     // 是否唯一索引
}

// BPlusTreeNode B+树节点
type BPlusTreeNode struct {
	IsLeaf   bool             // 是否是叶子节点
	Keys     []string         // 键值
	Children []*BPlusTreeNode // 子节点（非叶子节点）
	Values   [][]string       // 记录ID列表（叶子节点）
	Next     *BPlusTreeNode   // 下一个叶子节点的指针（叶子节点）
	Prev     *BPlusTreeNode   // 上一个叶子节点的指针（叶子节点，用于反向遍历）
	Parent   *BPlusTreeNode   // 父节点指针（用于快速向上遍历）
	Height   int              // 节点高度（根节点为0）
	Mutex    sync.RWMutex     // 节点级锁，提高并发性能
	Dirty    bool             // 标记节点是否被修改
}

// Index 表示一个索引
type Index struct {
	Config     IndexConfig                        // 索引配置
	Root       *BPlusTreeNode                     // B+树根节点
	mutex      sync.RWMutex                       // 读写锁
	Degree     int                                // B+树的度
	Height     int                                // 树的高度
	Size       int                                // 索引中的键数量
	NodeCache  *lru.Cache[string, *BPlusTreeNode] // 节点缓存
	LastAccess time.Time                          // 最后访问时间
	Stats      *IndexStats                        // 索引统计信息
}

// IndexStats 索引统计信息
// 扩展 IndexStats 结构体
type IndexStats struct {
    Lookups            int64         // 查找操作次数
    RangeLookups       int64         // 范围查找操作次数
    Inserts            int64         // 插入操作次数
    Deletes            int64         // 删除操作次数
    Splits             int64         // 节点分裂次数
    Merges             int64         // 节点合并次数
    CacheHits          int64         // 缓存命中次数
    CacheMisses        int64         // 缓存未命中次数
    AvgLookupTime      time.Duration // 平均查找时间
    MaxLookupTime      time.Duration // 最大查找时间
    MinLookupTime      time.Duration // 最小查找时间
    AvgInsertTime      time.Duration // 平均插入时间
    MaxInsertTime      time.Duration // 最大插入时间
    MinInsertTime      time.Duration // 最小插入时间
    AvgRangeLookupTime time.Duration // 平均范围查找时间
    MaxRangeLookupTime time.Duration // 最大范围查找时间
    AvgRangeSize       int           // 平均范围大小
    Resizes            int64         // 哈希表调整大小次数
    LastResizeTime     time.Duration // 最后一次调整大小耗时
    AvgBucketSize      float64       // 平均桶大小
    MaxBucketSize      int           // 最大桶大小
    EmptyBuckets       int           // 空桶数量
    BucketStdDev       float64       // 桶大小标准差
    Mutex              sync.Mutex    // 统计信息锁
}

// IndexMonitor 索引监控器
type IndexMonitor struct {
    manager         *IndexManager
    monitorInterval time.Duration
    stopCh          chan struct{}
}

// NewIndexMonitor 创建新的索引监控器
func NewIndexMonitor(manager *IndexManager) *IndexMonitor {
    return &IndexMonitor{
        manager:         manager,
        monitorInterval: 10 * time.Minute,
        stopCh:          make(chan struct{}),
    }
}

// Start 启动索引监控
func (im *IndexMonitor) Start() {
    go im.monitorRoutine()
    log.Println("索引监控器已启动")
}

// Stop 停止索引监控
func (im *IndexMonitor) Stop() {
    close(im.stopCh)
    log.Println("索引监控器已停止")
}

// monitorRoutine 监控例程
func (im *IndexMonitor) monitorRoutine() {
    ticker := time.NewTicker(im.monitorInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            im.collectStats()
            im.optimizeIndexes()
        case <-im.stopCh:
            return
        }
    }
}

// collectStats 收集索引统计信息
func (im *IndexMonitor) collectStats() {
    // 获取所有索引
    indexes := im.manager.GetAllIndexes()
    
    for _, index := range indexes {
        if index.Stats == nil {
            continue
        }
        
        // 收集统计信息
        index.Stats.Mutex.Lock()
        stats := *index.Stats // 复制统计信息
        index.Stats.Mutex.Unlock()
        
        // 记录统计信息
        log.Printf("索引 %s.%s 统计: 查询=%d, 插入=%d, 删除=%d, 平均查询时间=%v",
            index.Config.Table, index.Config.Name,
            stats.Lookups, stats.Inserts, stats.Deletes,
            stats.AvgLookupTime)
    }
}

// optimizeIndexes 优化索引
func (im *IndexMonitor) optimizeIndexes() {
    // 获取所有索引
    indexes := im.manager.GetAllIndexes()
    
    for _, index := range indexes {
        // 检查索引是否需要优化
        if index.needsOptimization() {
            log.Printf("开始优化索引 %s.%s", index.Config.Table, index.Config.Name)
            
            // 根据索引类型执行不同的优化
            switch index.Config.Type {
            case BPlusTreeIndex:
                index.optimizeBPlusTree()
            case HashIndex:
                if hashIndex, ok := index.HashIndex.(*HashIndex); ok {
                    hashIndex.optimizeBuckets()
                }
            }
        }
    }
}

Mutex         sync.Mutex    // 统计信息锁
}

// IndexManager 管理数据库中的所有索引
type IndexManager struct {
	indexes map[string]*Index // 索引名称 -> 索引
	db      *DB               // 数据库引用
	mutex   sync.RWMutex      // 读写锁
}