package storage

import (
	"sync"
)

// splitLeaf 分裂叶子节点
func (bt *BTree) splitLeaf(leaf *BTreeNode) {
	// 计算分裂点
	midIndex := len(leaf.Keys) / 2

	// 创建新节点
	newLeaf := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]string, len(leaf.Keys)-midIndex),
		Values:     make([][]string, len(leaf.Values)-midIndex),
		Next:       leaf.Next,
		Parent:     leaf.Parent,
		keyMutexes: make([]sync.RWMutex, len(leaf.Keys)-midIndex), // 初始化键级别锁
	}

	// 复制数据到新节点
	copy(newLeaf.Keys, leaf.Keys[midIndex:])
	copy(newLeaf.Values, leaf.Values[midIndex:])

	// 更新原节点
	leaf.Keys = leaf.Keys[:midIndex]
	leaf.Values = leaf.Values[:midIndex]
	leaf.keyMutexes = leaf.keyMutexes[:midIndex] // 更新键级别锁数组
	leaf.Next = newLeaf

	// 更新父节点
	bt.insertIntoParent(leaf, newLeaf, newLeaf.Keys[0])

	// 更新节点计数
	bt.NodeCount++
	
	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Splits++
		bt.Stats.mutex.Unlock()
	}
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

// splitNode 分裂内部节点
func (bt *BTree) splitNode(node *BTreeNode) {
	// 计算分裂点
	midIndex := len(node.Keys) / 2
	
	// 创建新节点
	newNode := &BTreeNode{
		IsLeaf:     false,
		Keys:       make([]string, len(node.Keys)-midIndex-1),
		Children:   make([]*BTreeNode, len(node.Children)-midIndex-1),
		keyMutexes: make([]sync.RWMutex, len(node.Keys)-midIndex-1),
	}
	
	// 获取中间键，用于插入父节点
	midKey := node.Keys[midIndex]
	
	// 复制后半部分键和子节点到新节点
	copy(newNode.Keys, node.Keys[midIndex+1:])
	copy(newNode.Children, node.Children[midIndex+1:])
	
	// 更新原节点
	node.Keys = node.Keys[:midIndex]
	node.Children = node.Children[:midIndex+1]
	node.keyMutexes = node.keyMutexes[:midIndex]
	
	// 更新父节点
	bt.insertInParent(node, midKey, newNode)
	
	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Splits++
		bt.Stats.NodeCount++
		bt.Stats.mutex.Unlock()
	}
}

// findParent 查找节点的父节点
func (bt *BTree) findParent(current, target *BTreeNode) *BTreeNode {
	if current == target || current.IsLeaf {
		return nil
	}
	
	// 检查当前节点的子节点
	for _, child := range current.Children {
		if child == target {
			return current
		}
	}
	
	// 递归查找
	for _, child := range current.Children {
		if parent := bt.findParent(child, target); parent != nil {
			return parent
		}
	}
	
	return nil
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
	
	// 查找左节点在父节点中的位置
	leftPos := -1
	for i, child := range parent.Children {
		if child == leftNode {
			leftPos = i
			break
		}
	}
	
	// 插入键和右节点
	// 为新键和子节点腾出空间
	parent.Keys = append(parent.Keys, "")
	parent.Children = append(parent.Children, nil)
	parent.keyMutexes = append(parent.keyMutexes, sync.RWMutex{})
	
	// 移动元素
	copy(parent.Keys[leftPos+1:], parent.Keys[leftPos:])
	copy(parent.Children[leftPos+2:], parent.Children[leftPos+1:])
	copy(parent.keyMutexes[leftPos+1:], parent.keyMutexes[leftPos:])
	
	// 插入新键和右节点
	parent.Keys[leftPos] = key
	parent.Children[leftPos+1] = rightNode
	
	// 检查父节点是否需要分裂
	if len(parent.Keys) > 2*bt.Degree-1 {
		bt.splitNode(parent)
	}
}

// redistributeKeys 重新分配节点中的键
func (bt *BTree) redistributeKeys(node *BTreeNode) {
	if node == nil || len(node.Children) <= 1 {
		return
	}
	
	// 获取所有子节点的键和值
	var allKeys []string
	var allValues [][]string
	var allChildren []*BTreeNode
	
	for _, child := range node.Children {
		child.mutex.Lock()
		defer child.mutex.Unlock()
		
		if child.IsLeaf {
			allKeys = append(allKeys, child.Keys...)
			allValues = append(allValues, child.Values...)
		} else {
			for i, grandchild := range child.Children {
				if i < len(child.Keys) {
					allKeys = append(allKeys, child.Keys[i])
				}
				allChildren = append(allChildren, grandchild)
			}
		}
	}
	
	// 计算每个节点应该有的键数量
	keysPerNode := len(allKeys) / len(node.Children)
	if keysPerNode < bt.Degree-1 {
		keysPerNode = bt.Degree - 1
	}
	
	// 重新分配键和值
	keyIndex := 0
	valueIndex := 0
	childIndex := 0
	
	for i, child := range node.Children {
		// 清空子节点
		child.Keys = child.Keys[:0]
		
		if child.IsLeaf {
			child.Values = child.Values[:0]
			
			// 分配键和值
			endIndex := keyIndex + keysPerNode
			if i == len(node.Children)-1 {
				endIndex = len(allKeys)
			}
			
			for j := keyIndex; j < endIndex && j < len(allKeys); j++ {
				child.Keys = append(child.Keys, allKeys[j])
				child.Values = append(child.Values, allValues[valueIndex])
				valueIndex++
			}
			
			keyIndex = endIndex
		} else {
			child.Children = child.Children[:0]
			
			// 分配键和子节点
			endIndex := keyIndex + keysPerNode
			if i == len(node.Children)-1 {
				endIndex = len(allKeys)
			}
			
			// 添加第一个子节点
			child.Children = append(child.Children, allChildren[childIndex])
			childIndex++
			
			for j := keyIndex; j < endIndex && j < len(allKeys); j++ {
				child.Keys = append(child.Keys, allKeys[j])
				child.Children = append(child.Children, allChildren[childIndex])
				childIndex++
			}
			
			keyIndex = endIndex
		}
	}
	
	// 更新父节点的键
	node.Keys = node.Keys[:0]
	for i := 0; i < len(node.Children)-1; i++ {
		if node.Children[i+1].IsLeaf {
			node.Keys = append(node.Keys, node.Children[i+1].Keys[0])
		} else if len(node.Children[i+1].Children) > 0 {
			// 使用子节点的第一个键作为分隔键
			node.Keys = append(node.Keys, node.Children[i+1].Keys[0])
		}
	}
}