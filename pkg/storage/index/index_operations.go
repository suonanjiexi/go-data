package storage

import (
	"fmt"
	"time"
)

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

// Lookup 使用索引查找记录
func (idx *Index) Lookup(key string) ([]string, time.Duration, error) {
    startTime := time.Now()
    
    idx.mutex.RLock()
    defer idx.mutex.RUnlock()
    
    if idx.Config.Type == BPlusTreeIndex {
        result, err := idx.lookupInBPlusTree(key)
        return result, time.Since(startTime), err
    }
    
    // 其他索引类型的处理...
    return nil, time.Since(startTime), nil
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