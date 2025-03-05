package storage

import (
	"sync"
)

// Iterator B+树迭代器
type Iterator struct {
	tree       *BTree
	currentNode *BTreeNode
	currentIndex int
	mutex      sync.RWMutex
}

// NewIterator 创建一个新的B+树迭代器
func (bt *BTree) NewIterator() *Iterator {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	
	// 查找最左侧的叶子节点
	leftmost := bt.findLeftmostLeaf(bt.Root)
	
	return &Iterator{
		tree:        bt,
		currentNode: leftmost,
		currentIndex: 0,
	}
}

// NewIteratorFrom 从指定键开始创建迭代器
func (bt *BTree) NewIteratorFrom(startKey string) *Iterator {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	
	// 查找起始叶子节点
	leaf := bt.findLeaf(startKey)
	
	// 查找起始键在叶子节点中的位置
	index := 0
	for i, key := range leaf.Keys {
		if key >= startKey {
			index = i
			break
		}
		
		if i == len(leaf.Keys)-1 {
			// 如果所有键都小于起始键，则从下一个叶子节点开始
			if leaf.Next != nil {
				leaf = leaf.Next
				index = 0
			}
		}
	}
	
	return &Iterator{
		tree:        bt,
		currentNode: leaf,
		currentIndex: index,
	}
}

// HasNext 检查是否有下一个元素
func (it *Iterator) HasNext() bool {
	it.mutex.RLock()
	defer it.mutex.RUnlock()
	
	if it.currentNode == nil {
		return false
	}
	
	// 当前节点还有更多键
	if it.currentIndex < len(it.currentNode.Keys) {
		return true
	}
	
	// 检查是否有下一个叶子节点
	return it.currentNode.Next != nil
}

// Next 获取下一个键值对
func (it *Iterator) Next() (string, []string, bool) {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	
	if it.currentNode == nil {
		return "", nil, false
	}
	
	// 当前节点还有更多键
	if it.currentIndex < len(it.currentNode.Keys) {
		key := it.currentNode.Keys[it.currentIndex]
		value := it.currentNode.Values[it.currentIndex]
		it.currentIndex++
		return key, value, true
	}
	
	// 移动到下一个叶子节点
	if it.currentNode.Next != nil {
		it.currentNode = it.currentNode.Next
		it.currentIndex = 0
		
		if len(it.currentNode.Keys) > 0 {
			key := it.currentNode.Keys[0]
			value := it.currentNode.Values[0]
			it.currentIndex++
			return key, value, true
		}
	}
	
	return "", nil, false
}

// Seek 定位到指定键
func (it *Iterator) Seek(key string) bool {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	
	// 查找叶子节点
	it.tree.mutex.RLock()
	leaf := it.tree.findLeaf(key)
	it.tree.mutex.RUnlock()
	
	if leaf == nil {
		it.currentNode = nil
		it.currentIndex = 0
		return false
	}
	
	// 查找键在叶子节点中的位置
	found := false
	index := 0
	for i, k := range leaf.Keys {
		if k >= key {
			index = i
			found = (k == key)
			break
		}
		
		if i == len(leaf.Keys)-1 {
			// 如果所有键都小于目标键，则从下一个叶子节点开始
			if leaf.Next != nil {
				leaf = leaf.Next
				index = 0
			}
		}
	}
	
	it.currentNode = leaf
	it.currentIndex = index
	return found
}

// Reset 重置迭代器到起始位置
func (it *Iterator) Reset() {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	
	it.tree.mutex.RLock()
	it.currentNode = it.tree.findLeftmostLeaf(it.tree.Root)
	it.tree.mutex.RUnlock()
	it.currentIndex = 0
}

// Close 关闭迭代器
func (it *Iterator) Close() {
	it.mutex.Lock()
	defer it.mutex.Unlock()
	
	it.currentNode = nil
	it.currentIndex = 0
}

// ForEach 对每个键值对执行操作
func (bt *BTree) ForEach(fn func(key string, value []string) bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	
	// 查找最左侧的叶子节点
	node := bt.findLeftmostLeaf(bt.Root)
	
	// 遍历所有叶子节点
	for node != nil {
		node.mutex.RLock()
		
		// 遍历当前节点的所有键值对
		for i, key := range node.Keys {
			if !fn(key, node.Values[i]) {
				node.mutex.RUnlock()
				return
			}
		}
		
		nextNode := node.Next
		node.mutex.RUnlock()
		node = nextNode
	}
}

// RangeForEach 对指定范围内的键值对执行操作
func (bt *BTree) RangeForEach(startKey, endKey string, fn func(key string, value []string) bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	
	// 查找起始叶子节点
	node := bt.findLeaf(startKey)
	
	// 遍历叶子节点
	for node != nil {
		node.mutex.RLock()
		
		// 遍历当前节点的键值对
		for i, key := range node.Keys {
			// 检查键是否在范围内
			if key >= startKey && (endKey == "" || key <= endKey) {
				if !fn(key, node.Values[i]) {
					node.mutex.RUnlock()
					return
				}
			}
			
			// 如果已经超过结束键，提前结束
			if endKey != "" && key > endKey {
				node.mutex.RUnlock()
				return
			}
		}
		
		nextNode := node.Next
		node.mutex.RUnlock()
		node = nextNode
	}
}