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
type IndexStats struct {
	Lookups       int64         // 查找操作次数
	RangeLookups  int64         // 范围查找操作次数
	Inserts       int64         // 插入操作次数
	Deletes       int64         // 删除操作次数
	Splits        int64         // 节点分裂次数
	Merges        int64         // 节点合并次数
	CacheHits     int64         // 缓存命中次数
	CacheMisses   int64         // 缓存未命中次数
	AvgLookupTime time.Duration // 平均查找时间
	MaxLookupTime time.Duration // 最大查找时间
	MinLookupTime time.Duration // 最小查找时间
	Mutex         sync.Mutex    // 统计信息锁
}

// IndexManager 管理数据库中的所有索引
type IndexManager struct {
	indexes map[string]*Index // 索引名称 -> 索引
	db      *DB               // 数据库引用
	mutex   sync.RWMutex      // 读写锁
}