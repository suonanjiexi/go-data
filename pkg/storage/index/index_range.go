package storage

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"
)

// RangeLookup 使用索引进行范围查找
func (idx *Index) RangeLookup(start, end string) ([]string, time.Duration, error) {
	startTime := time.Now()
	
	// 使用读锁，允许并发读取
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	
	// 更新统计信息
	if idx.Stats != nil {
		idx.Stats.Mutex.Lock()
		idx.Stats.RangeLookups++
		idx.Stats.Mutex.Unlock()
	}
	
	if idx.Config.Type != BPlusTreeIndex {
		return nil, time.Since(startTime), fmt.Errorf("range lookup only supported for B+ tree indexes")
	}
	
	if idx.Root == nil {
		return []string{}, time.Since(startTime), nil
	}
	
	// 使用并发处理大范围查询
	if end != "" && end > start && idx.Size > 10000 {
		return idx.concurrentRangeLookup(start, end, startTime)
	}
	
	// 预分配结果切片，减少内存分配
	result := make([]string, 0, 1000)
	
	node := idx.findLeaf(start)
	for node != nil {
		node.Mutex.RLock()
		
		// 使用二分查找优化
		startIdx := sort.SearchStrings(node.Keys, start)
		
		for i := startIdx; i < len(node.Keys); i++ {
			if end != "" && node.Keys[i] > end {
				node.Mutex.RUnlock()
				return result, time.Since(startTime), nil
			}
			result = append(result, node.Values[i]...)
		}
		
		nextNode := node.Next
		node.Mutex.RUnlock()
		node = nextNode
	}
	
	return result, time.Since(startTime), nil
}

// concurrentRangeLookup 并发范围查找
func (idx *Index) concurrentRangeLookup(start, end string, startTime time.Time) ([]string, time.Duration, error) {
	// 找到起始和结束节点
	startNode := idx.findLeaf(start)
	endNode := idx.findLeaf(end)
	
	if startNode == nil {
		return []string{}, time.Since(startTime), nil
	}
	
	// 计算节点数量和并发度
	nodeCount := 0
	currentNode := startNode
	for currentNode != nil && (endNode == nil || currentNode != endNode.Next) {
		nodeCount++
		currentNode = currentNode.Next
	}
	
	// 根据节点数量和CPU核心数确定并发度
	concurrency := runtime.NumCPU()
	if nodeCount < concurrency {
		concurrency = nodeCount
	}
	
	// 每个goroutine处理的节点数
	nodesPerWorker := nodeCount / concurrency
	if nodesPerWorker < 1 {
		nodesPerWorker = 1
	}
	
	// 创建结果通道和等待组
	resultChan := make(chan []string, concurrency)
	var wg sync.WaitGroup
	
	// 启动工作goroutine
	currentNode = startNode
	for i := 0; i < concurrency && currentNode != nil; i++ {
		wg.Add(1)
		
		// 确定当前工作goroutine的结束节点
		var endNodeForWorker *BPlusTreeNode
		if i == concurrency-1 {
			// 最后一个工作goroutine处理到最终结束节点
			endNodeForWorker = endNode
		} else {
			// 计算当前工作goroutine的结束节点
			endNodeForWorker = currentNode
			for j := 0; j < nodesPerWorker && endNodeForWorker != nil && endNodeForWorker.Next != nil; j++ {
				endNodeForWorker = endNodeForWorker.Next
			}
		}
		
		// 启动工作goroutine
		go func(startNode, endNode *BPlusTreeNode, startKey, endKey string) {
			defer wg.Done()
			
			// 预分配结果切片
			localResult := make([]string, 0, 1000)
			
			// 处理节点
			node := startNode
			for node != nil && (endNode == nil || node != endNode.Next) {
				node.Mutex.RLock()
				
				// 确定起始索引
				startIdx := 0
				if node == startNode {
					startIdx = sort.SearchStrings(node.Keys, startKey)
				}
				
				// 处理当前节点中的键
				for i := startIdx; i < len(node.Keys); i++ {
					if endKey != "" && node.Keys[i] > endKey {
						break
					}
					localResult = append(localResult, node.Values[i]...)
				}
				
				nextNode := node.Next
				node.Mutex.RUnlock()
				node = nextNode
			}
			
			// 发送结果
			resultChan <- localResult
		}(currentNode, endNodeForWorker, start, end)
		
		// 移动到下一个起始节点
		currentNode = endNodeForWorker.Next
	}
	
	// 等待所有工作goroutine完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// 合并结果
	result := make([]string, 0, 10000)
	for partialResult := range resultChan {
		result = append(result, partialResult...)
	}
	
	return result, time.Since(startTime), nil
}

// PrefixLookup 使用索引进行前缀查找
func (idx *Index) PrefixLookup(prefix string) ([]string, time.Duration, error) {
	startTime := time.Now()
	
	// 使用读锁，允许并发读取
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	
	if idx.Config.Type != BPlusTreeIndex {
		return nil, time.Since(startTime), fmt.Errorf("prefix lookup only supported for B+ tree indexes")
	}
	
	if idx.Root == nil {
		return []string{}, time.Since(startTime), nil
	}
	
	// 预分配结果切片
	result := make([]string, 0, 1000)
	
	// 找到前缀的起始节点
	node := idx.findLeaf(prefix)
	for node != nil {
		node.Mutex.RLock()
		
		// 使用二分查找找到前缀的起始位置
		startIdx := sort.SearchStrings(node.Keys, prefix)
		
		// 收集所有以前缀开头的键的值
		for i := startIdx; i < len(node.Keys); i++ {
			// 检查键是否以前缀开头
			if len(node.Keys[i]) < len(prefix) || node.Keys[i][:len(prefix)] != prefix {
				if i > startIdx {
					// 如果已经找到了一些匹配项，但当前键不匹配，说明已经超出了前缀范围
					node.Mutex.RUnlock()
					return result, time.Since(startTime), nil
				}
				// 如果第一个检查的键就不匹配，继续检查下一个键
				continue
			}
			
			// 添加匹配的值
			result = append(result, node.Values[i]...)
		}
		
		// 移动到下一个节点
		nextNode := node.Next
		node.Mutex.RUnlock()
		node = nextNode
	}
	
	return result, time.Since(startTime), nil
}

// GetStats 获取索引统计信息
func (idx *Index) GetStats() *IndexStats {
	if idx.Stats == nil {
		return &IndexStats{}
	}
	
	// 创建统计信息的副本
	idx.Stats.Mutex.Lock()
	defer idx.Stats.Mutex.Unlock()
	
	statsCopy := &IndexStats{
		Lookups:       idx.Stats.Lookups,
		RangeLookups:  idx.Stats.RangeLookups,
		Inserts:       idx.Stats.Inserts,
		Deletes:       idx.Stats.Deletes,
		Splits:        idx.Stats.Splits,
		Merges:        idx.Stats.Merges,
		CacheHits:     idx.Stats.CacheHits,
		CacheMisses:   idx.Stats.CacheMisses,
		AvgLookupTime: idx.Stats.AvgLookupTime,
		MaxLookupTime: idx.Stats.MaxLookupTime,
		MinLookupTime: idx.Stats.MinLookupTime,
	}
	
	return statsCopy
}

// ResetStats 重置索引统计信息
func (idx *Index) ResetStats() {
	if idx.Stats == nil {
		idx.Stats = &IndexStats{}
		return
	}
	
	idx.Stats.Mutex.Lock()
	defer idx.Stats.Mutex.Unlock()
	
	idx.Stats.Lookups = 0
	idx.Stats.RangeLookups = 0
	idx.Stats.Inserts = 0
	idx.Stats.Deletes = 0
	idx.Stats.Splits = 0
	idx.Stats.Merges = 0
	idx.Stats.CacheHits = 0
	idx.Stats.CacheMisses = 0
	idx.Stats.AvgLookupTime = 0
	idx.Stats.MaxLookupTime = 0
	idx.Stats.MinLookupTime = 0
}