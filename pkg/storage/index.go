package storage

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
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
	Mutex         sync.Mutex    // 统计信息锁
}

// IndexManager 管理数据库中的所有索引
type IndexManager struct {
	indexes map[string]*Index // 索引名称 -> 索引
	db      *DB               // 数据库引用
	mutex   sync.RWMutex      // 读写锁
}

// NewIndexManager 创建一个新的索引管理器
func NewIndexManager(db *DB) *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
		db:      db,
	}
}

// CreateIndex 创建一个新索引
func (im *IndexManager) CreateIndex(config IndexConfig) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 验证索引配置
	if config.Name == "" || config.Table == "" || len(config.Columns) == 0 {
		return ErrInvalidIndex
	}

	// 检查索引类型是否有效
	if config.Type != BPlusTreeIndex && config.Type != HashIndex {
		return ErrInvalidIndexType
	}

	// 检查索引是否已存在
	indexKey := fmt.Sprintf("%s:%s", config.Table, config.Name)
	if _, exists := im.indexes[indexKey]; exists {
		return ErrIndexExists
	}

	// 创建新索引
	index := &Index{
		Config: config,
		Root:   nil,
		Degree: 4, // 默认B+树度为4
	}

	// 初始化节点缓存
	nodeCache, err := lru.New[string, *BPlusTreeNode](1000) // 默认缓存1000个节点
	if err != nil {
		return fmt.Errorf("failed to create node cache: %w", err)
	}
	index.NodeCache = nodeCache

	// 初始化统计信息
	index.Stats = &IndexStats{}

	// 初始化最后访问时间
	index.LastAccess = time.Now()

	// 初始化B+树根节点
	if config.Type == BPlusTreeIndex {
		index.Root = &BPlusTreeNode{
			IsLeaf: true,
			Keys:   make([]string, 0),
			Values: make([][]string, 0),
		}
	}

	// 将索引添加到管理器
	im.indexes[indexKey] = index

	// 构建索引
	err := im.buildIndex(index)
	if err != nil {
		delete(im.indexes, indexKey)
		return err
	}

	// 持久化索引元数据
	return im.saveIndexMetadata(index)
}

// AddEntry 向索引添加条目
func (idx *Index) AddEntry(key string, recordID string) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.Config.Type == BPlusTreeIndex {
		return idx.addToBPlusTree(key, recordID)
	}

	// 其他索引类型的处理...
	return nil
}

// addToBPlusTree 向B+树中添加条目
func (idx *Index) addToBPlusTree(key string, recordID string) error {
	if idx.Root == nil {
		idx.Root = &BPlusTreeNode{
			IsLeaf: true,
			Keys:   []string{key},
			Values: [][]string{{recordID}},
		}
		return nil
	}

	// 查找合适的叶子节点
	leaf := idx.findLeaf(key)

	// 在叶子节点中插入键值对
	for i := 0; i < len(leaf.Keys); i++ {
		if leaf.Keys[i] == key {
			// 检查唯一性约束
			if idx.Config.Unique {
				return fmt.Errorf("unique index violation: key '%s' already exists", key)
			}
			// 添加记录ID
			leaf.Values[i] = append(leaf.Values[i], recordID)
			return nil
		}
		if leaf.Keys[i] > key {
			// 插入新键值对
			leaf.Keys = append(leaf.Keys, "")
			copy(leaf.Keys[i+1:], leaf.Keys[i:])
			leaf.Keys[i] = key
			leaf.Values = append(leaf.Values, nil)
			copy(leaf.Values[i+1:], leaf.Values[i:])
			leaf.Values[i] = []string{recordID}
			return idx.splitIfNeeded(leaf)
		}
	}

	// 添加到叶子节点末尾
	leaf.Keys = append(leaf.Keys, key)
	leaf.Values = append(leaf.Values, []string{recordID})
	return idx.splitIfNeeded(leaf)
}

// findLeaf 查找键所在的叶子节点
func (idx *Index) findLeaf(key string) *BPlusTreeNode {
	// 记录开始时间，用于统计
	startTime := time.Now()

	// 如果有统计信息，增加查找计数
	if idx.Stats != nil {
		idx.Stats.Mutex.Lock()
		idx.Stats.Lookups++
		idx.Stats.Mutex.Unlock()
	}

	node := idx.Root

	// 使用二分查找优化查找过程
	for !node.IsLeaf {
		// 获取节点级读锁
		node.Mutex.RLock()

		// 二分查找优化
		l, r := 0, len(node.Keys)-1
		i := 0

		if len(node.Keys) > 8 { // 只有当键数量足够多时才使用二分查找
			for l <= r {
				mid := (l + r) / 2
				if node.Keys[mid] <= key {
					i = mid + 1
					l = mid + 1
				} else {
					r = mid - 1
				}
			}
		} else {
			// 对于小节点，线性查找更快
			for i < len(node.Keys) && key >= node.Keys[i] {
				i++
			}
		}

		// 获取下一个节点
		nextNode := node.Children[i]
		node.Mutex.RUnlock()
		node = nextNode
	}

	// 更新统计信息
	if idx.Stats != nil {
		elapsed := time.Since(startTime)
		idx.Stats.Mutex.Lock()
		// 更新平均查找时间
		idx.Stats.AvgLookupTime = time.Duration((int64(idx.Stats.AvgLookupTime)*idx.Stats.Lookups + int64(elapsed)) / (idx.Stats.Lookups + 1))
		idx.Stats.Mutex.Unlock()
	}

	return node
}

// splitIfNeeded 在必要时分裂节点
func (idx *Index) splitIfNeeded(node *BPlusTreeNode) error {
	if len(node.Keys) <= 2*idx.Degree {
		return nil
	}

	// 分裂节点的逻辑
	midIndex := len(node.Keys) / 2
	newNode := &BPlusTreeNode{
		IsLeaf: node.IsLeaf,
		Keys:   make([]string, len(node.Keys)-midIndex),
		Values: make([][]string, len(node.Values)-midIndex),
	}

	copy(newNode.Keys, node.Keys[midIndex:])
	copy(newNode.Values, node.Values[midIndex:])

	// 更新原节点
	node.Keys = node.Keys[:midIndex]
	node.Values = node.Values[:midIndex]

	// 维护叶子节点链表
	if node.IsLeaf {
		newNode.Next = node.Next
		node.Next = newNode
	}

	// 更新父节点
	return idx.updateParent(node, newNode, newNode.Keys[0])
}

// updateParent 更新父节点
func (idx *Index) updateParent(node, newNode *BPlusTreeNode, key string) error {
	// 如果是根节点分裂，创建新的根节点
	if idx.Root == node {
		// 创建新的根节点
		newRoot := &BPlusTreeNode{
			IsLeaf:   false,
			Keys:     []string{key},
			Children: []*BPlusTreeNode{node, newNode},
		}
		idx.Root = newRoot
		return nil
	}

	// 查找父节点
	parent := idx.findParent(idx.Root, node)
	if parent == nil {
		return fmt.Errorf("parent node not found")
	}

	// 在父节点中插入新的键和子节点
	for i := 0; i < len(parent.Keys); i++ {
		if parent.Keys[i] > key {
			// 插入键
			parent.Keys = append(parent.Keys, "")
			copy(parent.Keys[i+1:], parent.Keys[i:])
			parent.Keys[i] = key

			// 插入子节点
			parent.Children = append(parent.Children, nil)
			copy(parent.Children[i+2:], parent.Children[i+1:])
			parent.Children[i+1] = newNode

			return idx.splitIfNeededInternal(parent)
		}
	}

	// 添加到父节点末尾
	parent.Keys = append(parent.Keys, key)
	parent.Children = append(parent.Children, newNode)

	return idx.splitIfNeededInternal(parent)
}

// Lookup 使用索引查找记录
func (idx *Index) Lookup(key string) ([]string, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	if idx.Config.Type == BPlusTreeIndex {
		return idx.lookupInBPlusTree(key)
	}

	// 其他索引类型的处理...
	return nil, nil
}

// lookupInBPlusTree 在B+树中查找记录
func (idx *Index) lookupInBPlusTree(key string) ([]string, error) {
	if idx.Root == nil {
		return []string{}, nil
	}

	leaf := idx.findLeaf(key)
	for i, k := range leaf.Keys {
		if k == key {
			result := make([]string, len(leaf.Values[i]))
			copy(result, leaf.Values[i])
			return result, nil
		}
	}

	return []string{}, nil
}

// RangeLookup 使用索引进行范围查找
func (idx *Index) RangeLookup(start, end string) ([]string, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	if idx.Config.Type != BPlusTreeIndex {
		return nil, fmt.Errorf("range lookup only supported for B+ tree indexes")
	}

	if idx.Root == nil {
		return []string{}, nil
	}

	// 找到起始叶子节点
	node := idx.findLeaf(start)
	result := []string{}

	// 遍历叶子节点链表
	for node != nil {
		for i, key := range node.Keys {
			if (start == "" || key >= start) && (end == "" || key <= end) {
				result = append(result, node.Values[i]...)
			}
			if end != "" && key > end {
				return result, nil
			}
		}
		node = node.Next
	}

	return result, nil
}

// findParent 查找节点的父节点
func (idx *Index) findParent(root *BPlusTreeNode, node *BPlusTreeNode) *BPlusTreeNode {
	if root == nil || root.IsLeaf {
		return nil
	}

	// 检查当前节点的子节点
	for _, child := range root.Children {
		if child == node {
			return root
		}
	}

	// 递归查找
	for _, child := range root.Children {
		if parent := idx.findParent(child, node); parent != nil {
			return parent
		}
	}

	return nil
}

// splitIfNeededInternal 在必要时分裂内部节点
func (idx *Index) splitIfNeededInternal(node *BPlusTreeNode) error {
	if len(node.Keys) <= 2*idx.Degree {
		return nil
	}

	// 分裂内部节点
	midIndex := len(node.Keys) / 2
	midKey := node.Keys[midIndex]

	// 创建新节点
	newNode := &BPlusTreeNode{
		IsLeaf:   false,
		Keys:     make([]string, len(node.Keys)-midIndex-1),
		Children: make([]*BPlusTreeNode, len(node.Children)-midIndex-1),
	}

	// 复制键和子节点到新节点
	copy(newNode.Keys, node.Keys[midIndex+1:])
	copy(newNode.Children, node.Children[midIndex+1:])

	// 更新原节点
	node.Keys = node.Keys[:midIndex]
	node.Children = node.Children[:midIndex+1]

	// 更新父节点
	return idx.updateParent(node, newNode, midKey)
}

// RemoveEntry 从索引中删除条目
func (idx *Index) RemoveEntry(key string, recordID string) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.Config.Type == BPlusTreeIndex {
		return idx.removeFromBPlusTree(key, recordID)
	}

	// 其他索引类型的处理...
	return nil
}

// removeFromBPlusTree 从B+树中删除条目
func (idx *Index) removeFromBPlusTree(key string, recordID string) error {
	if idx.Root == nil {
		return nil
	}

	// 查找叶子节点
	leaf := idx.findLeaf(key)

	// 在叶子节点中查找并删除记录ID
	for i, k := range leaf.Keys {
		if k == key {
			// 查找并删除记录ID
			newValues := []string{}
			for _, id := range leaf.Values[i] {
				if id != recordID {
					newValues = append(newValues, id)
				}
			}

			// 更新或删除键值对
			if len(newValues) > 0 {
				leaf.Values[i] = newValues
			} else {
				// 删除键值对
				leaf.Keys = append(leaf.Keys[:i], leaf.Keys[i+1:]...)
				leaf.Values = append(leaf.Values[:i], leaf.Values[i+1:]...)

				// 如果节点为空且不是根节点，考虑合并节点
				if len(leaf.Keys) == 0 && leaf != idx.Root {
					// 查找父节点
					parent := idx.findParent(idx.Root, leaf)
					if parent != nil {
						// 从父节点中移除指向当前节点的引用
						for j, child := range parent.Children {
							if child == leaf {
								// 移除子节点
								parent.Children = append(parent.Children[:j], parent.Children[j+1:]...)
								// 如果不是第一个子节点，也需要移除对应的键
								if j > 0 {
									parent.Keys = append(parent.Keys[:j-1], parent.Keys[j:]...)
								}
								break
							}
						}
					}

					// 更新叶子节点链表
					if leaf.Prev != nil {
						leaf.Prev.Next = leaf.Next
					}
					if leaf.Next != nil {
						leaf.Next.Prev = leaf.Prev
					}
				}
			}

			// 如果根节点为空，且有子节点，更新根节点
			if len(idx.Root.Keys) == 0 && !idx.Root.IsLeaf {
				if len(idx.Root.Children) > 0 {
					idx.Root = idx.Root.Children[0]
					idx.Root.Parent = nil
				}
			}

			// 更新统计信息
			if idx.Stats != nil {
				idx.Stats.Mutex.Lock()
				idx.Stats.Deletes++
				idx.Stats.Mutex.Unlock()
			}

			return nil
		}
	}

	return nil
}

// buildIndex 为现有数据构建索引
func (im *IndexManager) buildIndex(index *Index) error {
	// 获取表中所有记录
	// 这里应该实现遍历表中所有记录并构建索引的逻辑
	// 简化实现，实际应该使用迭代器或范围查询
	// TODO: 实现完整的索引构建逻辑
	return nil
}

// saveIndexMetadata 保存索引元数据
func (im *IndexManager) saveIndexMetadata(index *Index) error {
	// 构建索引元数据键
	metaKey := fmt.Sprintf("index:%s:%s", index.Config.Table, index.Config.Name)

	// 序列化索引配置
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(index.Config); err != nil {
		return err
	}

	// 保存到数据库
	return im.db.Put(metaKey, buf.Bytes())
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(table, name string) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 检查索引是否存在
	if _, exists := im.indexes[indexKey]; !exists {
		return ErrIndexNotFound
	}

	// 从管理器中删除索引
	delete(im.indexes, indexKey)

	// 删除索引元数据
	metaKey := fmt.Sprintf("index:%s:%s", table, name)
	return im.db.Delete(metaKey)
}

// GetIndex 获取指定索引
func (im *IndexManager) GetIndex(table, name string) (*Index, error) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 查找索引
	index, exists := im.indexes[indexKey]
	if !exists {
		return nil, ErrIndexNotFound
	}

	return index, nil
}

// LoadIndexes 从数据库加载所有索引
func (im *IndexManager) LoadIndexes() error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 清空现有索引
	im.indexes = make(map[string]*Index)

	// TODO: 实现从数据库加载索引元数据的逻辑
	// 这需要遍历所有以"index:"开头的键，解码索引配置，并重建索引

	return nil
}
