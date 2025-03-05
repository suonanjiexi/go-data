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