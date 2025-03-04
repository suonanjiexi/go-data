package storage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

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
	Inserts      int64         // 插入操作次数
	Deletes      int64         // 删除操作次数
	Queries      int64         // 查询操作次数
	Splits       int64         // 节点分裂次数
	Merges       int64         // 节点合并次数
	CacheHits    int64         // 缓存命中次数
	CacheMisses  int64         // 缓存未命中次数
	AvgQueryTime time.Duration // 平均查询时间
	mutex        sync.Mutex    // 统计信息的互斥锁
}

// NewBTree 创建一个新的B+树
func NewBTree(degree int) *BTree {
	if degree < 2 {
		degree = 2 // 最小度为2
	}

	// 创建根节点
	root := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]string, 0),
		Values:     make([][]string, 0),
		keyMutexes: make([]sync.RWMutex, 0), // 初始化键级别锁数组
	}

	// 创建缓存
	nodeCache, _ := lru.New[string, *BTreeNode](10000) // 增大缓存容量
	
	// 创建统计信息
	stats := &TreeStats{}

	return &BTree{
		Root:      root,
		Degree:    degree,
		NodeCount: 1,
		Height:    1,
		NodeCache: nodeCache,
		Stats:     stats,
	}
}

// Insert 插入键值对
func (bt *BTree) Insert(key string, recordID string, unique bool) error {
	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Inserts++
		bt.Stats.mutex.Unlock()
	}

	bt.mutex.RLock() // 使用读锁而不是写锁，提高并发性
	// 查找合适的叶子节点
	leaf := bt.findLeaf(key)
	bt.mutex.RUnlock()

	// 在叶子节点中查找位置
	leaf.mutex.Lock()

	// 确保keyMutexes数组与Keys数组长度一致
	for len(leaf.keyMutexes) < len(leaf.Keys) {
		leaf.keyMutexes = append(leaf.keyMutexes, sync.RWMutex{})
	}

	pos := 0
	for pos < len(leaf.Keys) && leaf.Keys[pos] < key {
		pos++
	}

	// 检查是否已存在相同的键
	if pos < len(leaf.Keys) && leaf.Keys[pos] == key {
		// 获取特定键的锁
		leaf.keyMutexes[pos].Lock()
		leaf.mutex.Unlock() // 释放节点锁，允许其他键的操作继续
		defer leaf.keyMutexes[pos].Unlock()

		// 如果是唯一索引，返回错误
		if unique {
			return fmt.Errorf("unique index violation: key '%s' already exists", key)
		}
		// 否则，添加记录ID
		leaf.Values[pos] = append(leaf.Values[pos], recordID)
		return nil
	}

	// 需要修改节点结构，保持节点锁
	defer leaf.mutex.Unlock()

	// 插入新键值对
	leaf.Keys = append(leaf.Keys, "")
	leaf.Values = append(leaf.Values, nil)
	leaf.keyMutexes = append(leaf.keyMutexes, sync.RWMutex{})

	// 移动元素，为新键值对腾出位置
	if pos < len(leaf.Keys)-1 {
		copy(leaf.Keys[pos+1:], leaf.Keys[pos:len(leaf.Keys)-1])
		copy(leaf.Values[pos+1:], leaf.Values[pos:len(leaf.Values)-1])
		// 不需要移动互斥锁，它们是值类型
	}

	// 插入新键值对
	leaf.Keys[pos] = key
	leaf.Values[pos] = []string{recordID}

	// 检查是否需要分裂
	if len(leaf.Keys) > 2*bt.Degree {
		// 需要获取树锁进行分裂操作
		bt.mutex.Lock()
		bt.splitLeaf(leaf)
		bt.mutex.Unlock()
	}

	return nil
}

// Search 查找键对应的记录ID列表
func (bt *BTree) Search(key string) (*QueryResult, error) {
	start := time.Now()

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Queries++
		bt.Stats.mutex.Unlock()
	}

	// 检查缓存
	if bt.NodeCache != nil {
		cacheKey := "search:" + key
		if cachedResult, ok := bt.NodeCache.Get(cacheKey); ok {
			// 缓存命中
			if bt.Stats != nil {
				bt.Stats.mutex.Lock()
				bt.Stats.CacheHits++
				bt.Stats.mutex.Unlock()
			}
			return cachedResult.(*QueryResult), nil
		}
	}

	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 查找叶子节点
	leaf := bt.findLeaf(key)

	// 在叶子节点中查找键
	leaf.mutex.RLock()
	defer leaf.mutex.RUnlock()

	// 更新节点访问时间
	leaf.LastAccess = time.Now()

	// 二分查找优化
	i := sort.Search(len(leaf.Keys), func(i int) bool {
		return leaf.Keys[i] >= key
	})

	if i < len(leaf.Keys) && leaf.Keys[i] == key {
		// 返回记录ID的副本
		result := make([]string, len(leaf.Values[i]))
		copy(result, leaf.Values[i])
		queryResult := &QueryResult{
			RecordIDs:   result,
			ElapsedTime: time.Since(start),
		}

		// 缓存结果
		if bt.NodeCache != nil {
			cacheKey := "search:" + key
			bt.NodeCache.Add(cacheKey, queryResult)
		}

		return queryResult, nil
	}

	// 未找到
	return &QueryResult{
		RecordIDs:   []string{},
		ElapsedTime: time.Since(start),
	}, nil
}

// InSearch 实现IN查询功能
func (bt *BTree) InSearch(keys []string) (*QueryResult, error) {
	start := time.Now()

	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	result := make([]string, 0)

	// 对每个键进行查询
	for _, key := range keys {
		// 查找叶子节点
		leaf := bt.findLeaf(key)

		// 在叶子节点中查找键
		leaf.mutex.RLock()
		for i, k := range leaf.Keys {
			if k == key {
				result = append(result, leaf.Values[i]...)
				break
			}
		}
		leaf.mutex.RUnlock()
	}

	return &QueryResult{
		RecordIDs:   result,
		ElapsedTime: time.Since(start),
	}, nil
}

// RangeSearch 范围查询
func (bt *BTree) RangeSearch(startKey, endKey string) (*QueryResult, error) {
	start := time.Now()

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Queries++
		bt.Stats.RangeLookups++
		bt.Stats.mutex.Unlock()
	}

	// 检查缓存
	if bt.NodeCache != nil {
		cacheKey := fmt.Sprintf("range:%s:%s", startKey, endKey)
		if cachedResult, ok := bt.NodeCache.Get(cacheKey); ok {
			// 缓存命中
			if bt.Stats != nil {
				bt.Stats.mutex.Lock()
				bt.Stats.CacheHits++
				bt.Stats.mutex.Unlock()
			}
			return cachedResult.(*QueryResult), nil
		}
	}

	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 预分配结果切片，减少内存分配
	result := make([]string, 0, 100)

	// 查找起始叶子节点
	node := bt.findLeaf(startKey)

	// 遍历叶子节点链表
	for node != nil {
		node.mutex.RLock()

		// 更新节点访问时间
		node.LastAccess = time.Now()

		// 二分查找找到起始位置
		startIdx := 0
		if startKey != "" {
			startIdx = sort.Search(len(node.Keys), func(i int) bool {
				return node.Keys[i] >= startKey
			})
		}

		// 处理当前节点中的键
		for i := startIdx; i < len(node.Keys); i++ {
			key := node.Keys[i]
			if endKey != "" && key > endKey {
				node.mutex.RUnlock()

				queryResult := &QueryResult{
					RecordIDs:   result,
					ElapsedTime: time.Since(start),
				}

				// 缓存结果
				if bt.NodeCache != nil {
					cacheKey := fmt.Sprintf("range:%s:%s", startKey, endKey)
					bt.NodeCache.Add(cacheKey, queryResult)
				}

				return queryResult, nil
			}

			// 添加匹配的记录ID
			result = append(result, node.Values[i]...)
		}

		// 获取下一个节点
		nextNode := node.Next
		node.mutex.RUnlock()
		node = nextNode
	}

	queryResult := &QueryResult{
		RecordIDs:   result,
		ElapsedTime: time.Since(start),
	}

	// 缓存结果
	if bt.NodeCache != nil {
		cacheKey := fmt.Sprintf("range:%s:%s", startKey, endKey)
		bt.NodeCache.Add(cacheKey, queryResult)
	}

	return queryResult, nil
}

// findLeaf 查找键所在的叶子节点
func (bt *BTree) findLeaf(key string) *BTreeNode {
	// 记录开始时间，用于性能统计
	start := time.Now()

	// 检查节点缓存
	if bt.NodeCache != nil {
		cacheKey := "node:" + key
		if cachedNode, ok := bt.NodeCache.Get(cacheKey); ok {
			// 缓存命中
			if bt.Stats != nil {
				bt.Stats.mutex.Lock()
				bt.Stats.CacheHits++
				bt.Stats.mutex.Unlock()
			}
			return cachedNode.(*BTreeNode)
		}
	}

	node := bt.Root
	var path []*BTreeNode // 记录查找路径，用于预取和缓存

	for !node.IsLeaf {
		// 获取节点级读锁
		node.mutex.RLock()
		
		// 记录路径
		path = append(path, node)

		// 优化的二分查找
		keysLen := len(node.Keys)
		pos := 0

		if keysLen > 8 { // 只有当键数量足够多时才使用二分查找
			// 使用更高效的二分查找
			l, r := 0, keysLen-1
			for l <= r {
				mid := l + (r-l)/2 // 避免整数溢出
				if node.Keys[mid] <= key {
					pos = mid + 1
					l = mid + 1
				} else {
					r = mid - 1
				}
			}
		} else {
			// 对于小节点，线性查找更快
			for pos < keysLen && key >= node.Keys[pos] {
				pos++
			}
		}

		// 获取下一个节点
		nextNode := node.Children[pos]
		
		// 预取相邻节点（如果可能是下一个查询目标）
		if bt.NodeCache != nil && pos+1 < len(node.Children) {
			go func(prefetchNode *BTreeNode) {
				prefetchCacheKey := fmt.Sprintf("node:%p", prefetchNode)
				bt.NodeCache.Add(prefetchCacheKey, prefetchNode)
			}(node.Children[pos+1])
		}
		
		node.mutex.RUnlock()
		node = nextNode
	}

	// 缓存叶子节点和路径上的节点
	if bt.NodeCache != nil {
		// 缓存叶子节点
		cacheKey := "node:" + key
		bt.NodeCache.Add(cacheKey, node)
		
		// 缓存路径上的节点，使用节点指针作为部分键
		for i, pathNode := range path {
			pathCacheKey := fmt.Sprintf("path:%s:%d", key, i)
			bt.NodeCache.Add(pathCacheKey, pathNode)
		}
	}

	// 更新统计信息
	if bt.Stats != nil {
		elapsed := time.Since(start)
		bt.Stats.mutex.Lock()
		// 更新查询性能统计
		bt.Stats.AvgSearchTime = (bt.Stats.AvgSearchTime*float64(bt.Stats.Queries) + elapsed.Seconds()) / float64(bt.Stats.Queries+1)
		// 记录最大查找时间
		if elapsed.Seconds() > bt.Stats.MaxSearchTime {
			bt.Stats.MaxSearchTime = elapsed.Seconds()
		}
		// 记录最小查找时间
		if bt.Stats.MinSearchTime == 0 || elapsed.Seconds() < bt.Stats.MinSearchTime {
			bt.Stats.MinSearchTime = elapsed.Seconds()
		}
		bt.Stats.mutex.Unlock()
	}

	return node
}

// splitLeaf 分裂叶子节点
func (bt *BTree) splitLeaf(leaf *BTreeNode) {
	// 计算分裂点
	midIndex := len(leaf.Keys) / 2

	// 创建新节点
	newLeaf := &BTreeNode{
		IsLeaf: true,
		Keys:   make([]string, len(leaf.Keys)-midIndex),
		Values: make([][]string, len(leaf.Values)-midIndex),
		Next:   leaf.Next,
		Parent: leaf.Parent,
	}

	// 复制数据到新节点
	copy(newLeaf.Keys, leaf.Keys[midIndex:])
	copy(newLeaf.Values, leaf.Values[midIndex:])

	// 更新原节点
	leaf.Keys = leaf.Keys[:midIndex]
	leaf.Values = leaf.Values[:midIndex]
	leaf.Next = newLeaf

	// 更新父节点
	bt.insertIntoParent(leaf, newLeaf, newLeaf.Keys[0])

	// 更新节点计数
	bt.NodeCount++
}

// insertIntoParent 将键和新节点插入到父节点
func (bt *BTree) insertIntoParent(left, right *BTreeNode, key string) {
	// 如果是根节点分裂，创建新的根节点
	if left.Parent == nil {
		// 创建新的根节点
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Keys:     []string{key},
			Children: []*BTreeNode{left, right},
		}

		// 更新子节点的父指针
		left.Parent = newRoot
		right.Parent = newRoot

		// 更新根节点
		bt.Root = newRoot
		bt.Height++
		bt.NodeCount++
		return
	}

	// 获取父节点
	parent := left.Parent
	parent.mutex.Lock()
	defer parent.mutex.Unlock()

	// 查找插入位置
	pos := 0
	for pos < len(parent.Children) && parent.Children[pos] != left {
		pos++
	}

	// 插入键
	parent.Keys = append(parent.Keys, "")
	if pos < len(parent.Keys) {
		copy(parent.Keys[pos+1:], parent.Keys[pos:])
	}
	parent.Keys[pos] = key

	// 插入子节点
	parent.Children = append(parent.Children, nil)
	if pos+1 < len(parent.Children)-1 {
		copy(parent.Children[pos+2:], parent.Children[pos+1:len(parent.Children)-1])
	}
	parent.Children[pos+1] = right
	right.Parent = parent

	// 检查是否需要分裂
	if len(parent.Keys) > 2*bt.Degree {
		bt.splitInternal(parent)
	}
}

// splitInternal 分裂内部节点
func (bt *BTree) splitInternal(node *BTreeNode) {
	// 计算分裂点
	midIndex := len(node.Keys) / 2
	midKey := node.Keys[midIndex]

	// 创建新节点
	newNode := &BTreeNode{
		IsLeaf:   false,
		Keys:     make([]string, len(node.Keys)-midIndex-1),
		Children: make([]*BTreeNode, len(node.Children)-midIndex-1),
		Parent:   node.Parent,
	}

	// 复制数据到新节点
	copy(newNode.Keys, node.Keys[midIndex+1:])
	copy(newNode.Children, node.Children[midIndex+1:])

	// 更新子节点的父指针
	for _, child := range newNode.Children {
		child.Parent = newNode
	}

	// 更新原节点
	node.Keys = node.Keys[:midIndex]
	node.Children = node.Children[:midIndex+1]

	// 更新父节点
	bt.insertIntoParent(node, newNode, midKey)

	// 更新节点计数
	bt.NodeCount++
}

// Delete 删除键对应的记录ID
func (bt *BTree) Delete(key string, recordID string) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 查找叶子节点
	leaf := bt.findLeaf(key)
	leaf.mutex.Lock()
	defer leaf.mutex.Unlock()

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Deletes++
		bt.Stats.mutex.Unlock()
	}

	// 在叶子节点中查找键
	for i, k := range leaf.Keys {
		if k == key {
			// 查找记录ID
			found := false
			for j, id := range leaf.Values[i] {
				if id == recordID {
					// 删除记录ID
					leaf.Values[i] = append(leaf.Values[i][:j], leaf.Values[i][j+1:]...)
					found = true
					break
				}
			}

			if !found {
				return fmt.Errorf("record ID '%s' not found for key '%s'", recordID, key)
			}

			// 如果没有更多记录ID，删除键
			if len(leaf.Values[i]) == 0 {
				// 删除键和值
				leaf.Keys = append(leaf.Keys[:i], leaf.Keys[i+1:]...)
				leaf.Values = append(leaf.Values[:i], leaf.Values[i+1:]...)

				// 检查是否需要合并或重新分配
				if len(leaf.Keys) < bt.Degree {
					bt.mergeOrRedistribute(leaf)
				}
			}

			// 清除缓存
			if bt.NodeCache != nil {
				cacheKey := "search:" + key
				bt.NodeCache.Remove(cacheKey)
			}

			return nil
		}
	}

	return fmt.Errorf("key '%s' not found", key)
}

// mergeOrRedistribute 合并或重新分配节点
func (bt *BTree) mergeOrRedistribute(node *BTreeNode) {
	// 如果是根节点，不需要合并
	if node == bt.Root {
		return
	}

	// 获取父节点
	parent := node.Parent
	if parent == nil {
		return
	}

	// 查找节点在父节点中的位置
	nodeIndex := -1
	for i, child := range parent.Children {
		if child == node {
			nodeIndex = i
			break
		}
	}

	// 如果找不到节点，返回
	if nodeIndex == -1 {
		return
	}

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Merges++
		bt.Stats.mutex.Unlock()
	}

	// 尝试从左兄弟节点借键
	if nodeIndex > 0 {
		leftSibling := parent.Children[nodeIndex-1]
		leftSibling.mutex.Lock()
		defer leftSibling.mutex.Unlock()

		// 如果左兄弟有足够的键，可以借用
		if len(leftSibling.Keys) > bt.Degree {
			// 借用左兄弟的最后一个键
			borrowKey := leftSibling.Keys[len(leftSibling.Keys)-1]
			borrowValue := leftSibling.Values[len(leftSibling.Values)-1]

			// 从左兄弟移除
			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]

			// 添加到当前节点
			node.Keys = append([]string{borrowKey}, node.Keys...)
			node.Values = append([][]string{borrowValue}, node.Values...)

			// 更新父节点中的分隔键
			parent.Keys[nodeIndex-1] = borrowKey

			return
		}
	}

	// 尝试从右兄弟节点借键
	if nodeIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[nodeIndex+1]
		rightSibling.mutex.Lock()
		defer rightSibling.mutex.Unlock()

		// 如果右兄弟有足够的键，可以借用
		if len(rightSibling.Keys) > bt.Degree {
			// 借用右兄弟的第一个键
			borrowKey := rightSibling.Keys[0]
			borrowValue := rightSibling.Values[0]

			// 从右兄弟移除
			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Values = rightSibling.Values[1:]

			// 添加到当前节点
			node.Keys = append(node.Keys, borrowKey)
			node.Values = append(node.Values, borrowValue)

			// 更新父节点中的分隔键
			parent.Keys[nodeIndex] = rightSibling.Keys[0]

			return
		}
	}

	// 如果无法借用，需要合并节点
	// 优先与左兄弟合并
	if nodeIndex > 0 {
		leftSibling := parent.Children[nodeIndex-1]
		leftSibling.mutex.Lock()
		defer leftSibling.mutex.Unlock()

		// 将当前节点的键和值合并到左兄弟
		leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
		leftSibling.Values = append(leftSibling.Values, node.Values...)

		// 更新叶子节点链表
		if node.IsLeaf {
			leftSibling.Next = node.Next
			if node.Next != nil {
				node.Next.Prev = leftSibling
			}
		}

		// 从父节点中移除当前节点
		parent.Keys = append(parent.Keys[:nodeIndex-1], parent.Keys[nodeIndex:]...)
		parent.Children = append(parent.Children[:nodeIndex], parent.Children[nodeIndex+1:]...)

		// 如果父节点的键太少，递归合并
		if len(parent.Keys) < bt.Degree-1 && parent != bt.Root {
			bt.mergeOrRedistribute(parent)
		}
		
		// 如果父节点是根节点且没有键，更新根节点
		if parent == bt.Root && len(parent.Keys) == 0 {
			bt.Root = parent.Children[0]
			bt.Root.Parent = nil
			bt.Height--
		}
	}
}
			bt.mergeOrRedistribute(parent)
		}

		// 如果父节点是根节点且没有键，更新根节点
		if parent == bt.Root && len(parent.Keys) == 0 {
			bt.Root = parent.Children[0]
			bt.Root.Parent = nil
			bt.Height--
		}

		return
	}

	// 与右兄弟合并
	if nodeIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[nodeIndex+1]
		rightSibling.mutex.Lock()
		defer rightSibling.mutex.Unlock()

		// 将右兄弟的键和值合并到当前节点
		node.Keys = append(node.Keys, rightSibling.Keys...)
		node.Values = append(node.Values, rightSibling.Values...)

		// 更新叶子节点链表
		if node.IsLeaf {
			node.Next = rightSibling.Next
			if rightSibling.Next != nil {
				rightSibling.Next.Prev = node
			}
		}

		// 从父节点中移除右兄弟节点
		parent.Keys = append(parent.Keys[:nodeIndex], parent.Keys[nodeIndex+1:]...)
		parent.Children = append(parent.Children[:nodeIndex+1], parent.Children[nodeIndex+2:]...)

		// 如果父节点的键太少，递归合并
		if len(parent.Keys) < bt.Degree-1 && parent != bt.Root {
			bt.mergeOrRedistribute(parent)
		}
		
		// 如果父节点是根节点且没有键，更新根节点
		if parent == bt.Root && len(parent.Keys) == 0 {
			bt.Root = parent.Children[0]
			bt.Root.Parent = nil
			bt.Height--
		}
	}
}
			bt.mergeOrRedistribute(parent)
		}

		// 如果父节点是根节点且没有键，更新根节点
		if parent == bt.Root && len(parent.Keys) == 0 {
			bt.Root = parent.Children[0]
			bt.Root.Parent = nil
			bt.Height--
		}
	}
}

// handleUnderflow 处理节点下溢
func (bt *BTree) handleUnderflow(node *BTreeNode) {
	// 如果是根节点，特殊处理
	if node.Parent == nil {
		if !node.IsLeaf && len(node.Children) == 1 {
			// 根只有一个子节点，将子节点提升为新的根
			bt.Root = node.Children[0]
			bt.Root.Parent = nil
			bt.Height--
		}
		return
	}

	// 获取父节点和兄弟节点
	parent := node.Parent
	parent.mutex.Lock()
	defer parent.mutex.Unlock()

	// 查找节点在父节点中的位置
	nodeIndex := 0
	for nodeIndex < len(parent.Children) && parent.Children[nodeIndex] != node {
		nodeIndex++
	}

	// 尝试从左兄弟借节点
	if nodeIndex > 0 {
		leftSibling := parent.Children[nodeIndex-1]
		leftSibling.mutex.Lock()
		defer leftSibling.mutex.Unlock()

		if len(leftSibling.Keys) > bt.Degree {
			// 从左兄弟借一个节点
			if node.IsLeaf {
				// 叶子节点的情况
				// 将左兄弟的最后一个键值对移到当前节点
				node.Keys = append([]string{leftSibling.Keys[len(leftSibling.Keys)-1]}, node.Keys...)
				node.Values = append([][]string{leftSibling.Values[len(leftSibling.Values)-1]}, node.Values...)

				// 更新左兄弟
				leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
				leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]

				// 更新父节点中的分隔键
				parent.Keys[nodeIndex-1] = node.Keys[0]
			} else {
				// 内部节点的情况
				// 将父节点中的分隔键下移到当前节点
				node.Keys = append([]string{parent.Keys[nodeIndex-1]}, node.Keys...)

				// 将左兄弟的最后一个子节点移到当前节点
				node.Children = append([]*BTreeNode{leftSibling.Children[len(leftSibling.Children)-1]}, node.Children...)
				node.Children[0].Parent = node

				// 更新父节点中的分隔键
				parent.Keys[nodeIndex-1] = leftSibling.Keys[len(leftSibling.Keys)-1]

				// 更新左兄弟
				leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
				leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
			}
			return
		}
	}

	// 尝试从右兄弟借节点
	if nodeIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[nodeIndex+1]
		rightSibling.mutex.Lock()
		defer rightSibling.mutex.Unlock()

		if len(rightSibling.Keys) > bt.Degree {
			// 从右兄弟借一个节点
			if node.IsLeaf {
				// 叶子节点的情况
				// 将右兄弟的第一个键值对移到当前节点
				node.Keys = append(node.Keys, rightSibling.Keys[0])
				node.Values = append(node.Values, rightSibling.Values[0])

				// 更新右兄弟
				rightSibling.Keys = rightSibling.Keys[1:]
				rightSibling.Values = rightSibling.Values[1:]

				// 更新父节点中的分隔键
				parent.Keys[nodeIndex] = rightSibling.Keys[0]
			} else {
				// 内部节点的情况
				// 将父节点中的分隔键下移到当前节点
				node.Keys = append(node.Keys, parent.Keys[nodeIndex])

				// 将右兄弟的第一个子节点移到当前节点
				node.Children = append(node.Children, rightSibling.Children[0])
				node.Children[len(node.Children)-1].Parent = node

				// 更新父节点中的分隔键
				parent.Keys[nodeIndex] = rightSibling.Keys[0]

				// 更新右兄弟
				rightSibling.Keys = rightSibling.Keys[1:]
				rightSibling.Children = rightSibling.Children[1:]
			}
			return
		}
	}

	// 如果无法借节点，则需要合并
	// 优先与左兄弟合并
	if nodeIndex > 0 {
		leftSibling := parent.Children[nodeIndex-1]
		leftSibling.mutex.Lock()
		defer leftSibling.mutex.Unlock()

		// 合并节点
		if node.IsLeaf {
			// 叶子节点合并
			// 将当前节点的键值对追加到左兄弟
			leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
			leftSibling.Values = append(leftSibling.Values, node.Values...)

			// 更新链表指针
			leftSibling.Next = node.Next
		} else {
			// 内部节点合并
			// 将父节点中的分隔键下移到左兄弟
			leftSibling.Keys = append(leftSibling.Keys, parent.Keys[nodeIndex-1])

			// 将当前节点的键和子节点追加到左兄弟
			leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
			leftSibling.Children = append(leftSibling.Children, node.Children...)

			// 更新子节点的父指针
			for _, child := range node.Children {
				child.Parent = leftSibling
			}
		}

		// 从父节点中删除当前节点和分隔键
		parent.Keys = append(parent.Keys[:nodeIndex-1], parent.Keys[nodeIndex:]...)
		parent.Children = append(parent.Children[:nodeIndex], parent.Children[nodeIndex+1:]...)

		// 检查父节点是否需要处理下溢
		if len(parent.Keys) < bt.Degree-1 {
			bt.handleUnderflow(parent)
		}

		return
	}

	// 与右兄弟合并
	if nodeIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[nodeIndex+1]
		rightSibling.mutex.Lock()
		defer rightSibling.mutex.Unlock()

		// 合并节点
		if node.IsLeaf {
			// 叶子节点合并
			// 将右兄弟的键值对追加到当前节点
			node.Keys = append(node.Keys, rightSibling.Keys...)
			node.Values = append(node.Values, rightSibling.Values...)

			// 更新链表指针
			node.Next = rightSibling.Next
		} else {
			// 内部节点合并
			// 将父节点中的分隔键下移到当前节点
			node.Keys = append(node.Keys, parent.Keys[nodeIndex])

			// 将右兄弟的键和子节点追加到当前节点
			node.Keys = append(node.Keys, rightSibling.Keys...)
			node.Children = append(node.Children, rightSibling.Children...)

			// 更新子节点的父指针
			for _, child := range rightSibling.Children {
				child.Parent = node
			}
		}

		// 从父节点中删除右兄弟和分隔键
		parent.Keys = append(parent.Keys[:nodeIndex], parent.Keys[nodeIndex+1:]...)
		parent.Children = append(parent.Children[:nodeIndex+1], parent.Children[nodeIndex+2:]...)

		// 检查父节点是否需要处理下溢
		if len(parent.Keys) < bt.Degree-1 {
			bt.handleUnderflow(parent)
		}
	}
}
