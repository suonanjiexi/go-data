package storage

import (
	"fmt"
	"sync"
)

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
	defer leaf.mutex.Unlock()

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
		// 如果是唯一索引，返回错误
		if unique {
			return fmt.Errorf("unique index violation: key '%s' already exists", key)
		}
		
		// 检查记录ID是否已存在
		for _, existingID := range leaf.Values[pos] {
			if existingID == recordID {
				// 记录ID已存在，无需重复添加
				return nil
			}
		}
		
		// 添加记录ID
		leaf.Values[pos] = append(leaf.Values[pos], recordID)
		return nil
	}

	// 插入新键值对
	leaf.Keys = append(leaf.Keys, "")
	leaf.Values = append(leaf.Values, nil)
	leaf.keyMutexes = append(leaf.keyMutexes, sync.RWMutex{})

	// 移动元素，为新键值对腾出位置
	if pos < len(leaf.Keys)-1 {
		copy(leaf.Keys[pos+1:], leaf.Keys[pos:len(leaf.Keys)-1])
		copy(leaf.Values[pos+1:], leaf.Values[pos:len(leaf.Values)-1])
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

// Insert 插入键值对
func (bt *BTree) Insert(key string, value []string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 如果树为空，创建根节点
	if bt.Root == nil {
		bt.Root = &BTreeNode{
			IsLeaf:     true,
			Keys:       []string{key},
			Values:     [][]string{value},
			keyMutexes: make([]sync.RWMutex, 1),
		}
		bt.NodeCount = 1
		bt.Height = 1
		return
	}

	// 查找叶子节点
	leaf := bt.findLeaf(key)
	
	// 获取节点写锁
	leaf.mutex.Lock()
	defer leaf.mutex.Unlock()

	// 查找插入位置
	insertPos := 0
	for insertPos < len(leaf.Keys) && leaf.Keys[insertPos] < key {
		insertPos++
	}

	// 如果键已存在，更新值
	if insertPos < len(leaf.Keys) && leaf.Keys[insertPos] == key {
		leaf.Values[insertPos] = value
		return
	}

	// 插入新键值对
	leaf.Keys = append(leaf.Keys, "")
	leaf.Values = append(leaf.Values, nil)
	leaf.keyMutexes = append(leaf.keyMutexes, sync.RWMutex{})
	
	// 移动元素，为新键值对腾出空间
	copy(leaf.Keys[insertPos+1:], leaf.Keys[insertPos:])
	copy(leaf.Values[insertPos+1:], leaf.Values[insertPos:])
	copy(leaf.keyMutexes[insertPos+1:], leaf.keyMutexes[insertPos:])
	
	// 插入新键值对
	leaf.Keys[insertPos] = key
	leaf.Values[insertPos] = value

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Inserts++
		bt.Stats.mutex.Unlock()
	}

	// 检查是否需要分裂
	if len(leaf.Keys) > 2*bt.Degree-1 {
		bt.splitLeaf(leaf)
	}
}

// splitLeaf 分裂叶子节点
func (bt *BTree) splitLeaf(leaf *BTreeNode) {
	// 计算分裂点
	midIndex := len(leaf.Keys) / 2
	
	// 创建新节点
	newLeaf := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]string, len(leaf.Keys)-midIndex),
		Values:     make([][]string, len(leaf.Keys)-midIndex),
		keyMutexes: make([]sync.RWMutex, len(leaf.Keys)-midIndex),
		Next:       leaf.Next,
	}
	
	// 复制后半部分键值对到新节点
	copy(newLeaf.Keys, leaf.Keys[midIndex:])
	copy(newLeaf.Values, leaf.Values[midIndex:])
	
	// 更新原节点
	leaf.Keys = leaf.Keys[:midIndex]
	leaf.Values = leaf.Values[:midIndex]
	leaf.keyMutexes = leaf.keyMutexes[:midIndex]
	leaf.Next = newLeaf
	
	// 获取中间键，用于插入父节点
	midKey := newLeaf.Keys[0]
	
	// 更新父节点
	bt.insertInParent(leaf, midKey, newLeaf)
	
	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Splits++
		bt.Stats.NodeCount++
		bt.Stats.mutex.Unlock()
	}
}

// insertInParent 在父节点中插入键和子节点
func (bt *BTree) insertInParent(leftNode *BTreeNode, key string, rightNode *BTreeNode) {
	// 如果左节点是根节点，创建新的根节点
	if leftNode == bt.Root {
		newRoot := &BTreeNode{
			IsLeaf:     false,
			Keys:       []string{key},
			Children:   []*BTreeNode{leftNode, rightNode},
			keyMutexes: make([]sync.RWMutex, 1),
		}
		bt.Root = newRoot
		bt.Height++
		bt.NodeCount++
		return
	}
	
	// 查找父节点
	parent := bt.findParent(bt.Root, leftNode)
	
	// 获取父节点写锁
	parent.mutex.Lock()
	defer parent.mutex.Unlock()
	
	// 查找左节点在父节点中的位
```