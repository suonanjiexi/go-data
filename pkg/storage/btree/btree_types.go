package storage

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// BTreeNode B+树节点
type BTreeNode struct {
	IsLeaf     bool         // 是否为叶子节点
	Keys       []string     // 键数组
	Values     [][]string   // 值数组（叶子节点）
	Children   []*BTreeNode // 子节点指针（非叶子节点）
	Next       *BTreeNode   // 下一个叶子节点指针（用于范围查询）
	mutex      sync.RWMutex // 节点级锁
	keyMutexes []sync.RWMutex // 键级别锁数组
}

// BTree B+树结构
type BTree struct {
	Root      *BTreeNode                // 根节点
	Degree    int                       // B+树的度
	NodeCount int                       // 节点数量
	Height    int                       // 树高
	mutex     sync.RWMutex              // 树级锁
	NodeCache *lru.Cache[string, *BTreeNode] // 节点缓存
	Stats     *TreeStats                // 统计信息
}

// TreeStats B+树统计信息
type TreeStats struct {
	Queries       int64         // 查询次数
	Inserts       int64         // 插入次数
	Deletes       int64         // 删除次数
	Splits        int64         // 分裂次数
	Merges        int64         // 合并次数
	CacheHits     int64         // 缓存命中次数
	CacheMisses   int64         // 缓存未命中次数
	MinSearchTime float64       // 最小查找时间（秒）
	MaxSearchTime float64       // 最大查找时间（秒）
	AvgSearchTime float64       // 平均查找时间（秒）
	LastRebalance time.Time     // 最后一次重平衡时间
	mutex         sync.Mutex    // 统计信息锁
}

// SearchResult 查找结果
type SearchResult struct {
	Found  bool       // 是否找到
	Node   *BTreeNode // 节点
	Index  int        // 键在节点中的索引
	Values []string   // 值（如果找到）
}

// QueryResult 查询结果结构
type QueryResult struct {
	RecordIDs   []string      // 记录ID列表
	ElapsedTime time.Duration // 查询耗时
}

// BTreeNode B树节点
type BTreeNode struct {
	IsLeaf     bool         // 是否是叶子节点
	Keys       []string     // 键值
	Children   []*BTreeNode // 子节点（非叶子节点）
	Values     [][]string   // 记录ID列表（叶子节点）
	Next       *BTreeNode   // 下一个叶子节点的指针（叶子节点）
	Prev       *BTreeNode   // 上一个叶子节点的指针（用于反向遍历）
	Parent     *BTreeNode   // 父节点指针，优化查找父节点的性能
	Height     int          // 节点高度（根节点为0）
	mutex      sync.RWMutex // 节点级别的读写锁
	keyMutexes []sync.RWMutex // 键级别的读写锁，提供更细粒度的并发控制
	Dirty      bool         // 标记节点是否被修改
	CachedSum  int64        // 缓存的子节点值的总和（用于聚合查询优化）
	LastAccess time.Time    // 最后访问时间（用于缓存管理）
}

// BTree 优化的B+树实现
type BTree struct {
	Root      *BTreeNode                     // 根节点
	Degree    int                            // B+树的度
	mutex     sync.RWMutex                   // 树级别的读写锁
	NodeCount int                            // 节点计数，用于性能监控
	Height    int                            // 树高，用于性能监控
	NodeCache *lru.Cache[string, *BTreeNode] // 节点缓存
	Stats     *TreeStats                     // 树的统计信息
}

// TreeStats 树的统计信息
type TreeStats struct {
	Inserts       int64         // 插入操作次数
	Deletes       int64         // 删除操作次数
	Queries       int64         // 查询操作次数
	Splits        int64         // 节点分裂次数
	Merges        int64         // 节点合并次数
	CacheHits     int64         // 缓存命中次数
	CacheMisses   int64         // 缓存未命中次数
	AvgQueryTime  time.Duration // 平均查询时间
	RangeLookups  int64         // 范围查询次数
	AvgSearchTime float64       // 平均搜索时间
	MaxSearchTime float64       // 最大搜索时间
	MinSearchTime float64       // 最小搜索时间
	mutex         sync.Mutex    // 统计信息的互斥锁
}