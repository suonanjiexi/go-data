package storage

import (
	"fmt"
	"sort"
	"time"
)

// findLeaf 查找键所在的叶子节点
func (idx *Index) findLeaf(key string) *BPlusTreeNode {
	// 记录开始时间，用于统计
	startTime := time.Now()

	// 检查节点缓存
	if idx.NodeCache != nil {
		cacheKey := "leaf:" + key
		if cachedNode, ok := idx.NodeCache.Get(cacheKey); ok {
			// 缓存命中
			if idx.Stats != nil {
				idx.Stats.Mutex.Lock()
				idx.Stats.CacheHits++
				idx.Stats.Mutex.Unlock()
			}
			return cachedNode
		}
	}

	// 如果有统计信息，增加查找计数
	if idx.Stats != nil {
		idx.Stats.Mutex.Lock()
		idx.Stats.Lookups++
		idx.Stats.Mutex.Unlock()
	}

	node := idx.Root
	var path []*BPlusTreeNode // 记录查找路径，用于预取和缓存

	// 使用二分查找优化查找过程
	for !node.IsLeaf {
		// 获取节点级读锁
		node.Mutex.RLock()
		
		// 记录路径
		path = append(path, node)

		// 优化的二分查找
		keysLen := len(node.Keys)
		i := 0

		if keysLen > 8 { // 只有当键数量足够多时才使用二分查找
			// 使用更高效的二分查找
			l, r := 0, keysLen-1
			for l <= r {
				mid := l + (r-l)/2 // 避免整数溢出
				if node.Keys[mid] <= key {
					i = mid + 1
					l = mid + 1
				} else {
					r = mid - 1
				}
			}
		} else {
			// 对于小节点，使用优化的线性查找
			for i < keysLen && key >= node.Keys[i] {
				i++
			}
		}

		// 获取下一个节点
		nextNode := node.Children[i]
		
		// 预取相邻节点（如果可能是下一个查询目标）
		if idx.NodeCache != nil && i+1 < len(node.Children) {
			go func(prefetchNode *BPlusTreeNode) {
				prefetchCacheKey := fmt.Sprintf("node:%p", prefetchNode)
				idx.NodeCache.Add(prefetchCacheKey, prefetchNode)
			}(node.Children[i+1])
		}
		
		node.Mutex.RUnlock()
		node = nextNode
	}

	// 缓存叶子节点和路径上的节点
	if idx.NodeCache != nil {
		// 缓存叶子节点
		cacheKey := "leaf:" + key
		idx.NodeCache.Add(cacheKey, node)
		
		// 缓存路径上的节点，使用节点指针作为部分键
		for i, pathNode := range path {
			pathCacheKey := fmt.Sprintf("path:%s:%d", key, i)
			idx.NodeCache.Add(pathCacheKey, pathNode)
		}
	}

	// 更新统计信息
	if idx.Stats != nil {
		elapsed := time.Since(startTime)
		idx.Stats.Mutex.Lock()
		// 更新平均查找时间
		idx.Stats.AvgLookupTime = time.Duration((int64(idx.Stats.AvgLookupTime)*int64(idx.Stats.Lookups) + int64(elapsed)) / int64(idx.Stats.Lookups+1))
		// 记录最大查找时间
		if elapsed > idx.Stats.MaxLookupTime {
			idx.Stats.MaxLookupTime = elapsed
		}
		// 记录最小查找时间
		if idx.Stats.MinLookupTime == 0 || elapsed < idx.Stats.MinLookupTime {
			idx.Stats.MinLookupTime = elapsed
		}
		idx.Stats.Mutex.Unlock()
	}

	return node
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

// splitIfNeeded 在必要时分裂节点
func (idx *Index) splitIfNeeded(node *BPlusTreeNode) error {
    // 检查是否需要分裂
    if len(node.Keys) <= 2*idx.Degree {
        return nil
    }
    
    // 获取节点锁，确保分裂操作的原子性
    node.Mutex.Lock()
    defer node.Mutex.Unlock()
    
    // 再次检查是否需要分裂（可能在获取锁的过程中已经被其他线程分裂）
    if len(node.Keys) <= 2*idx.Degree {
        return nil
    }
    
    // 更新统计信息
    if idx.Stats != nil {
        idx.Stats.Mutex.Lock()
        idx.Stats.Splits++
        idx.Stats.Mutex.Unlock()
    }
    
    // 分裂节点的逻辑
    midIndex := len(node.Keys) / 2
    newNode := &BPlusTreeNode{
        IsLeaf: node.IsLeaf,
        Keys:   make([]string, len(node.Keys)-midIndex),
        Values: make([][]string, len(node.Values)-midIndex),
        Height: node.Height,
        Dirty:  true,
    }
    
    copy(newNode.Keys, node.Keys[midIndex:])
    copy(newNode.Values, node.Values[midIndex:])
    
    // 更新原节点
    node.Keys = node.Keys[:midIndex]
    node.Values = node.Values[:midIndex]
    node.Dirty = true
    
    // 维护叶子节点链表
    if node.IsLeaf {
        newNode.Next = node.Next
        if newNode.Next != nil {
            newNode.Next.Prev = newNode
        }
        node.Next = newNode
        newNode.Prev = node
    } else {
        // 对于非叶子节点，需要更新子节点的父指针
        for _, child := range newNode.Children {
            child.Parent = newNode
        }
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

// 优化 B+树节点结构，增加节点锁和缓存支持
type BPlusTreeNode struct {
    IsLeaf   bool
    Keys     []string
    Values   [][]string
    Children []*BPlusTreeNode
    Next     *BPlusTreeNode  // 用于叶子节点链表
    Parent   *BPlusTreeNode  // 父节点引用，优化向上遍历
    mutex    sync.RWMutex    // 节点级锁，提高并发性能
    dirty    bool            // 标记节点是否被修改，用于缓存管理
}

// 增强 Find 方法，支持并发安全的查找
func (idx *Index) Find(key string) ([]string, error) {
    startTime := time.Now()
    
    // 使用读锁保护根节点
    idx.mutex.RLock()
    root := idx.Root
    idx.mutex.RUnlock()
    
    if root == nil {
        return nil, fmt.Errorf("index is empty")
    }
    
    // 查找叶子节点
    leaf := idx.findLeaf(key)
    if leaf == nil {
        return nil, fmt.Errorf("leaf node not found")
    }
    
    // 在叶子节点中查找键
    leaf.mutex.RLock()
    defer leaf.mutex.RUnlock()
    
    for i, k := range leaf.Keys {
        if k == key {
            // 更新统计信息
            idx.updateStats(time.Since(startTime))
            return leaf.Values[i], nil
        }
    }
    
    return nil, fmt.Errorf("key not found: %s", key)
}

// 优化 Insert 方法，支持并发插入
// Insert 插入键值对到B+树索引
func (idx *Index) Insert(key string, value string) error {
    startTime := time.Now()
    
    // 如果是唯一索引，先检查键是否已存在
    if idx.Config.Unique {
        if values, err := idx.Find(key); err == nil && len(values) > 0 {
            return fmt.Errorf("唯一索引冲突: %s", key)
        }
    }
    
    // 获取根节点的读锁
    idx.mutex.RLock()
    root := idx.Root
    idx.mutex.RUnlock()
    
    // 如果根节点为空，创建新的根节点
    if root == nil {
        idx.mutex.Lock()
        // 再次检查根节点是否为空（可能在获取锁的过程中被其他线程创建）
        if idx.Root == nil {
            idx.Root = &BPlusTreeNode{
                IsLeaf: true,
                Keys:   []string{key},
                Values: [][]string{{value}},
            }
            idx.mutex.Unlock()
            
            // 更新统计信息
            idx.updateInsertStats(time.Since(startTime))
            return nil
        }
        idx.mutex.Unlock()
        
        // 根节点已被创建，继续正常插入流程
        root = idx.Root
    }
    
    // 查找要插入的叶子节点
    leaf := idx.findLeaf(key)
    
    // 获取叶子节点的写锁
    leaf.mutex.Lock()
    defer leaf.mutex.Unlock()
    
    // 在叶子节点中查找插入位置
    pos := sort.SearchStrings(leaf.Keys, key)
    
    // 检查键是否已存在
    if pos < len(leaf.Keys) && leaf.Keys[pos] == key {
        // 键已存在，添加值（如果不是唯一索引）
        if !idx.Config.Unique {
            // 检查值是否已存在
            for _, v := range leaf.Values[pos] {
                if v == value {
                    // 值已存在，无需重复添加
                    return nil
                }
            }
            
            // 添加新值
            leaf.Values[pos] = append(leaf.Values[pos], value)
            leaf.Dirty = true
            
            // 更新统计信息
            idx.updateInsertStats(time.Since(startTime))
            return nil
        }
        
        // 唯一索引，返回错误
        return fmt.Errorf("唯一索引冲突: %s", key)
    }
    
    // 键不存在，插入新键值对
    // 扩展键和值数组
    leaf.Keys = append(leaf.Keys, "")
    leaf.Values = append(leaf.Values, nil)
    
    // 移动元素，为新键值对腾出位置
    if pos < len(leaf.Keys)-1 {
        copy(leaf.Keys[pos+1:], leaf.Keys[pos:len(leaf.Keys)-1])
        copy(leaf.Values[pos+1:], leaf.Values[pos:len(leaf.Values)-1])
    }
    
    // 插入新键值对
    leaf.Keys[pos] = key
    leaf.Values[pos] = []string{value}
    leaf.Dirty = true
    
    // 更新索引大小
    idx.Size++
    
    // 检查是否需要分裂节点
    if len(leaf.Keys) > 2*idx.Degree {
        // 获取树的写锁进行分裂操作
        idx.mutex.Lock()
        err := idx.splitIfNeeded(leaf)
        idx.mutex.Unlock()
        if err != nil {
            return err
        }
    }
    
    // 更新统计信息
    idx.updateInsertStats(time.Since(startTime))
    return nil
}

// updateInsertStats 更新插入操作的统计信息
func (idx *Index) updateInsertStats(duration time.Duration) {
    if idx.Stats == nil {
        return
    }
    
    idx.Stats.Mutex.Lock()
    defer idx.Stats.Mutex.Unlock()
    
    idx.Stats.Inserts++
    
    // 更新平均插入时间
    if idx.Stats.Inserts == 1 {
        idx.Stats.AvgInsertTime = duration
    } else {
        idx.Stats.AvgInsertTime = time.Duration(
            (int64(idx.Stats.AvgInsertTime)*(idx.Stats.Inserts-1) + int64(duration)) / 
            idx.Stats.Inserts)
    }
    
    // 更新最大插入时间
    if duration > idx.Stats.MaxInsertTime {
        idx.Stats.MaxInsertTime = duration
    }
    
    // 更新最小插入时间
    if idx.Stats.MinInsertTime == 0 || duration < idx.Stats.MinInsertTime {
        idx.Stats.MinInsertTime = duration
    }
}
    
    // 如果需要分裂节点
    if len(leaf.Keys) > idx.Order {
        return idx.splitLeaf(leaf)
    }
    
    return nil
}

// 新增范围查询优化方法
func (idx *Index) RangeQuery(startKey, endKey string) (map[string][]string, error) {
    startTime := time.Now()
    result := make(map[string][]string)
    
    // 查找起始叶子节点
    leaf := idx.findLeaf(startKey)
    if leaf == nil {
        return nil, fmt.Errorf("start leaf not found")
    }
    
    // 遍历叶子节点链表
    for leaf != nil {
        leaf.mutex.RLock()
        
        // 收集范围内的键值对
        for i, k := range leaf.Keys {
            if k >= startKey && (endKey == "" || k <= endKey) {
                result[k] = leaf.Values[i]
            }
            
            // 如果已经超过结束键，提前结束
            if endKey != "" && k > endKey {
                leaf.mutex.RUnlock()
                idx.updateStats(time.Since(startTime))
                return result, nil
            }
        }
        
        // 获取下一个叶子节点
        nextLeaf := leaf.Next
        leaf.mutex.RUnlock()
        leaf = nextLeaf
    }
    
    idx.updateStats(time.Since(startTime))
    return result, nil
}

// 新增前缀查询方法
func (idx *Index) PrefixQuery(prefix string) (map[string][]string, error) {
    startTime := time.Now()
    result := make(map[string][]string)
    
    // 查找起始叶子节点
    leaf := idx.findLeaf(prefix)
    if leaf == nil {
        return nil, fmt.Errorf("prefix leaf not found")
    }
    
    // 遍历叶子节点链表
    for leaf != nil {
        leaf.mutex.RLock()
        
        // 收集前缀匹配的键值对
        hasMatch := false
        for i, k := range leaf.Keys {
            if strings.HasPrefix(k, prefix) {
                result[k] = leaf.Values[i]
                hasMatch = true
            } else if hasMatch {
                // 如果已经找到过匹配项，且当前键不匹配，说明已经超出前缀范围
                leaf.mutex.RUnlock()
                idx.updateStats(time.Since(startTime))
                return result, nil
            }
        }
        
        // 获取下一个叶子节点
        nextLeaf := leaf.Next
        leaf.mutex.RUnlock()
        leaf = nextLeaf
    }
    
    idx.updateStats(time.Since(startTime))
    return result, nil
}
// needsOptimization 检查索引是否需要优化
func (idx *Index) needsOptimization() bool {
    // 如果索引为空，不需要优化
    if idx.Root == nil {
        return false
    }
    
    // 检查统计信息
    if idx.Stats == nil {
        return false
    }
    
    // 如果查询性能下降，需要优化
    if idx.Stats.AvgLookupTime > 10*time.Millisecond && idx.Stats.Lookups > 1000 {
        return true
    }
    
    // 如果树高度过大，需要优化
    if idx.Height > 5 {
        return true
    }
    
    return false
}

// optimizeBPlusTree 优化B+树索引
func (idx *Index) optimizeBPlusTree() {
    // 获取树的写锁
    idx.mutex.Lock()
    defer idx.mutex.Unlock()
    
    // 检查是否需要重新平衡树
    if idx.needsRebalancing() {
        idx.rebalanceTree()
    }
    
    // 检查是否需要压缩树
    if idx.needsCompaction() {
        idx.compactTree()
    }
    
    // 检查是否需要重建索引
    if idx.needsRebuild() {
        idx.rebuildTree()
    }
    
    log.Printf("索引 %s.%s 优化完成", idx.Config.Table, idx.Config.Name)
}

// needsRebalancing 检查是否需要重新平衡树
func (idx *Index) needsRebalancing() bool {
    // 检查树的高度是否过大
    if idx.Height > 5 {
        return true
    }
    
    // 检查叶子节点的填充率
    fillRate := idx.calculateLeafFillRate()
    if fillRate < 0.5 {
        return true
    }
    
    return false
}

// calculateLeafFillRate 计算叶子节点的平均填充率
func (idx *Index) calculateLeafFillRate() float64 {
    if idx.Root == nil {
        return 1.0
    }
    
    // 找到第一个叶子节点
    node := idx.Root
    for !node.IsLeaf {
        node = node.Children[0]
    }
    
    // 遍历所有叶子节点
    totalNodes := 0
    totalKeys := 0
    maxCapacity := 2 * idx.Degree
    
    for node != nil {
        totalNodes++
        totalKeys += len(node.Keys)
        node = node.Next
    }
    
    if totalNodes == 0 {
        return 1.0
    }
    
    return float64(totalKeys) / float64(totalNodes * maxCapacity)
}

// rebalanceTree 重新平衡B+树
func (idx *Index) rebalanceTree() {
    log.Printf("开始重新平衡索引 %s.%s", idx.Config.Table, idx.Config.Name)
    
    // 收集所有键值对
    keyValues := idx.collectAllKeyValues()
    
    // 重建树
    idx.rebuildFromKeyValues(keyValues)
    
    log.Printf("索引 %s.%s 重新平衡完成", idx.Config.Table, idx.Config.Name)
}

// collectAllKeyValues 收集所有键值对
func (idx *Index) collectAllKeyValues() map[string][]string {
    result := make(map[string][]string)
    
    // 如果树为空，直接返回
    if idx.Root == nil {
        return result
    }
    
    // 找到第一个叶子节点
    node := idx.Root
    for !node.IsLeaf {
        node = node.Children[0]
    }
    
    // 遍历所有叶子节点
    for node != nil {
        for i, key := range node.Keys {
            result[key] = node.Values[i]
        }
        node = node.Next
    }
    
    return result
}

// rebuildFromKeyValues 从键值对重建树
func (idx *Index) rebuildFromKeyValues(keyValues map[string][]string) {
    // 清空现有树
    idx.Root = nil
    idx.Height = 0
    
    // 如果没有键值对，直接返回
    if len(keyValues) == 0 {
        return
    }
    
    // 创建新的根节点
    idx.Root = &BPlusTreeNode{
        IsLeaf: true,
        Keys:   make([]string, 0),
        Values: make([][]string, 0),
    }
    
    // 按键排序
    keys := make([]string, 0, len(keyValues))
    for key := range keyValues {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    
    // 批量插入键值对
    batchSize := 1000
    for i := 0; i < len(keys); i += batchSize {
        end := i + batchSize
        if end > len(keys) {
            end = len(keys)
        }
        
        batch := keys[i:end]
        idx.batchInsert(batch, keyValues)
    }
}

// batchInsert 批量插入键值对
func (idx *Index) batchInsert(keys []string, keyValues map[string][]string) {
    for _, key := range keys {
        values := keyValues[key]
        for _, value := range values {
            // 使用优化的插入方法
            idx.insertWithoutRebalance(key, value)
        }
    }
    
    // 最后进行一次平衡操作
    idx.balanceTree()
}

// insertWithoutRebalance 不进行重平衡的插入操作
func (idx *Index) insertWithoutRebalance(key string, value string) {
    // 如果根节点为空，创建新的根节点
    if idx.Root == nil {
        idx.Root = &BPlusTreeNode{
            IsLeaf: true,
            Keys:   []string{key},
            Values: [][]string{{value}},
        }
        return
    }
    
    // 查找要插入的叶子节点
    leaf := idx.findLeaf(key)
    
    // 在叶子节点中查找插入位置
    pos := sort.SearchStrings(leaf.Keys, key)
    
    // 检查键是否已存在
    if pos < len(leaf.Keys) && leaf.Keys[pos] == key {
        // 键已存在，添加值（如果不是唯一索引）
        if !idx.Config.Unique {
            // 检查值是否已存在
            for _, v := range leaf.Values[pos] {
                if v == value {
                    // 值已存在，无需重复添加
                    return
                }
            }
            
            // 添加新值
            leaf.Values[pos] = append(leaf.Values[pos], value)
            leaf.Dirty = true
        }
        return
    }
    
    // 键不存在，插入新键值对
    // 扩展键和值数组
    leaf.Keys = append(leaf.Keys, "")
    leaf.Values = append(leaf.Values, nil)
    
    // 移动元素，为新键值对腾出位置
    if pos < len(leaf.Keys)-1 {
        copy(leaf.Keys[pos+1:], leaf.Keys[pos:len(leaf.Keys)-1])
        copy(leaf.Values[pos+1:], leaf.Values[pos:len(leaf.Values)-1])
    }
    
    // 插入新键值对
    leaf.Keys[pos] = key
    leaf.Values[pos] = []string{value}
    leaf.Dirty = true
    
    // 更新索引大小
    idx.Size++
}

// balanceTree 平衡整个树
func (idx *Index) balanceTree() {
    // 从叶子节点开始，自下而上平衡树
    if idx.Root == nil {
        return
    }
    
    // 找到第一个叶子节点
    node := idx.Root
    for !node.IsLeaf {
        node = node.Children[0]
    }
    
    // 平衡所有叶子节点
    for node != nil {
        if len(node.Keys) > 2*idx.Degree {
            idx.splitNode(node)
        }
        node = node.Next
    }
    
    // 平衡内部节点
    idx.balanceInternalNodes(idx.Root)
}

// balanceInternalNodes 平衡内部节点
func (idx *Index) balanceInternalNodes(node *BPlusTreeNode) {
    if node == nil || node.IsLeaf {
        return
    }
    
    // 平衡子节点
    for _, child := range node.Children {
        idx.balanceInternalNodes(child)
    }
    
    // 平衡当前节点
    if len(node.Keys) > 2*idx.Degree {
        idx.splitInternalNode(node)
    }
}

// splitNode 分裂节点
func (idx *Index) splitNode(node *BPlusTreeNode) {
    // 实现节点分裂逻辑
    // 这里可以复用之前的 splitIfNeeded 方法
    idx.splitIfNeeded(node)
}

// splitInternalNode 分裂内部节点
func (idx *Index) splitInternalNode(node *BPlusTreeNode) {
    // 实现内部节点分裂逻辑
    // 这里可以复用之前的 splitIfNeededInternal 方法
    idx.splitIfNeededInternal(node)
}

// needsCompaction 检查是否需要压缩树
func (idx *Index) needsCompaction() bool {
    // 检查删除操作是否过多
    if idx.Stats == nil {
        return false
    }
    
    // 如果删除操作超过插入操作的50%，需要压缩
    if idx.Stats.Deletes > idx.Stats.Inserts/2 {
        return true
    }
    
    return false
}

// compactTree 压缩B+树
func (idx *Index) compactTree() {
    log.Printf("开始压缩索引 %s.%s", idx.Config.Table, idx.Config.Name)
    
    // 收集所有有效的键值对
    keyValues := idx.collectAllKeyValues()
    
    // 重建树
    idx.rebuildFromKeyValues(keyValues)
    
    // 重置删除计数
    if idx.Stats != nil {
        idx.Stats.Mutex.Lock()
        idx.Stats.Deletes = 0
        idx.Stats.Mutex.Unlock()
    }
    
    log.Printf("索引 %s.%s 压缩完成", idx.Config.Table, idx.Config.Name)
}

// needsRebuild 检查是否需要重建索引
func (idx *Index) needsRebuild() bool {
    // 如果树高度过大或者查询性能严重下降，需要重建
    if idx.Height > 7 {
        return true
    }
    
    if idx.Stats != nil && idx.Stats.AvgLookupTime > 20*time.Millisecond && idx.Stats.Lookups > 10000 {
        return true
    }
    
    return false
}

// rebuildTree 重建B+树索引
func (idx *Index) rebuildTree() {
    log.Printf("开始重建索引 %s.%s", idx.Config.Table, idx.Config.Name)
    
    // 收集所有键值对
    keyValues := idx.collectAllKeyValues()
    
    // 优化树的度
    idx.optimizeDegree()
    
    // 重建树
    idx.rebuildFromKeyValues(keyValues)
    
    // 重置统计信息
    if idx.Stats != nil {
        idx.Stats.Mutex.Lock()
        idx.Stats.Lookups = 0
        idx.Stats.Inserts = 0
        idx.Stats.Deletes = 0
        idx.Stats.AvgLookupTime = 0
        idx.Stats.MaxLookupTime = 0
        idx.Stats.MinLookupTime = 0
        idx.Stats.Mutex.Unlock()
    }
    
    log.Printf("索引 %s.%s 重建完成", idx.Config.Table, idx.Config.Name)
}

// optimizeDegree 优化B+树的度
func (idx *Index) optimizeDegree() {
    // 根据索引大小和查询模式优化树的度
    if idx.Size < 1000 {
        // 小索引使用较小的度
        idx.Degree = 4
    } else if idx.Size < 10000 {
        // 中等大小的索引
        idx.Degree = 8
    } else if idx.Size < 100000 {
        // 大索引
        idx.Degree = 16
    } else {
        // 超大索引
        idx.Degree = 32
    }
    
    // 根据查询模式调整
    if idx.Stats != nil {
        // 如果范围查询多，使用较小的度
        if idx.Stats.RangeLookups > idx.Stats.Lookups {
            idx.Degree = max(4, idx.Degree/2)
        }
        
        // 如果点查询多，使用较大的度
        if idx.Stats.Lookups > idx.Stats.RangeLookups*10 {
            idx.Degree = min(64, idx.Degree*2)
        }
    }
    
    log.Printf("索引 %s.%s 度优化为 %d", idx.Config.Table, idx.Config.Name, idx.Degree)
}

// max 返回两个整数中的较大值
func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// min 返回两个整数中的较小值
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}