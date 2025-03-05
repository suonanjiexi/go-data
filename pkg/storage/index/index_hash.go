package index

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// HashIndex 实现哈希索引
type HashIndex struct {
	// 哈希表，键为哈希值，值为键值对列表
	buckets     []bucket
	// 哈希表大小
	size        int
	// 当前元素数量
	count       int
	// 负载因子阈值，超过此值触发扩容
	loadFactor  float64
	// 是否为唯一索引
	unique      bool
	// 统计信息
	stats       *IndexStats
	// 全局锁
	mutex       sync.RWMutex
	// 分片锁，提高并发性能
	bucketMutex []sync.RWMutex
}

// bucket 表示哈希桶，存储具有相同哈希值的键值对
type bucket struct {
	entries []entry
}

// entry 表示哈希表中的一个键值对
type entry struct {
	key    string
	values []string
}

// NewHashIndex 创建新的哈希索引
func NewHashIndex(initialSize int, unique bool) *HashIndex {
	if initialSize < 16 {
		initialSize = 16
	}
	
	// 创建分片锁
	bucketMutex := make([]sync.RWMutex, initialSize)
	
	// 创建哈希表
	buckets := make([]bucket, initialSize)
	for i := range buckets {
		buckets[i] = bucket{entries: make([]entry, 0, 4)}
	}
	
	return &HashIndex{
		buckets:     buckets,
		size:        initialSize,
		count:       0,
		loadFactor:  0.75,
		unique:      unique,
		stats:       NewIndexStats(),
		bucketMutex: bucketMutex,
	}
}

// hash 计算键的哈希值
func (idx *HashIndex) hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// getBucketIndex 获取键对应的桶索引
func (idx *HashIndex) getBucketIndex(key string) int {
	return int(idx.hash(key) % uint32(idx.size))
}

// Insert 插入键值对
func (idx *HashIndex) Insert(key string, value string) error {
	startTime := time.Now()
	
	// 计算桶索引
	bucketIdx := idx.getBucketIndex(key)
	
	// 获取桶锁
	idx.bucketMutex[bucketIdx].Lock()
	defer idx.bucketMutex[bucketIdx].Unlock()
	
	// 检查是否为唯一索引
	if idx.unique {
		for i := range idx.buckets[bucketIdx].entries {
			if idx.buckets[bucketIdx].entries[i].key == key {
				return fmt.Errorf("duplicate key in unique index: %s", key)
			}
		}
	}
	
	// 查找键是否已存在
	for i := range idx.buckets[bucketIdx].entries {
		if idx.buckets[bucketIdx].entries[i].key == key {
			// 键已存在，添加值
			idx.buckets[bucketIdx].entries[i].values = append(
				idx.buckets[bucketIdx].entries[i].values, value)
			
			// 更新统计信息
			idx.stats.mutex.Lock()
			idx.stats.Inserts++
			idx.stats.AvgInsertTime = (idx.stats.AvgInsertTime*float64(idx.stats.Inserts-1) + 
				float64(time.Since(startTime).Microseconds())) / float64(idx.stats.Inserts)
			idx.stats.mutex.Unlock()
			
			return nil
		}
	}
	
	// 键不存在，创建新条目
	idx.buckets[bucketIdx].entries = append(
		idx.buckets[bucketIdx].entries, 
		entry{key: key, values: []string{value}})
	
	// 更新计数
	idx.mutex.Lock()
	idx.count++
	
	// 检查是否需要扩容
	if float64(idx.count)/float64(idx.size) > idx.loadFactor {
		idx.mutex.Unlock()
		go idx.resize(idx.size * 2) // 异步扩容
	} else {
		idx.mutex.Unlock()
	}
	
	// 更新统计信息
	idx.stats.mutex.Lock()
	idx.stats.Inserts++
	idx.stats.AvgInsertTime = (idx.stats.AvgInsertTime*float64(idx.stats.Inserts-1) + 
		float64(time.Since(startTime).Microseconds())) / float64(idx.stats.Inserts)
	idx.stats.mutex.Unlock()
	
	return nil
}

// Find 查找键对应的值
func (idx *HashIndex) Find(key string) ([]string, error) {
	startTime := time.Now()
	
	// 计算桶索引
	bucketIdx := idx.getBucketIndex(key)
	
	// 获取桶读锁
	idx.bucketMutex[bucketIdx].RLock()
	defer idx.bucketMutex[bucketIdx].RUnlock()
	
	// 在桶中查找键
	for i := range idx.buckets[bucketIdx].entries {
		if idx.buckets[bucketIdx].entries[i].key == key {
			// 更新统计信息
			idx.stats.mutex.Lock()
			idx.stats.Queries++
			duration := time.Since(startTime)
			idx.stats.AvgQueryTime = (idx.stats.AvgQueryTime*float64(idx.stats.Queries-1) + 
				float64(duration.Microseconds())) / float64(idx.stats.Queries)
			if idx.stats.MinQueryTime == 0 || duration < idx.stats.MinQueryTime {
				idx.stats.MinQueryTime = duration
			}
			if duration > idx.stats.MaxQueryTime {
				idx.stats.MaxQueryTime = duration
			}
			idx.stats.mutex.Unlock()
			
			return idx.buckets[bucketIdx].entries[i].values, nil
		}
	}
	
	return nil, fmt.Errorf("key not found: %s", key)
}

// Delete 删除键值对
func (idx *HashIndex) Delete(key string, value string) error {
	startTime := time.Now()
	
	// 计算桶索引
	bucketIdx := idx.getBucketIndex(key)
	
	// 获取桶锁
	idx.bucketMutex[bucketIdx].Lock()
	defer idx.bucketMutex[bucketIdx].Unlock()
	
	// 在桶中查找键
	for i := range idx.buckets[bucketIdx].entries {
		if idx.buckets[bucketIdx].entries[i].key == key {
			// 如果指定了值，只删除该值
			if value != "" {
				for j, v := range idx.buckets[bucketIdx].entries[i].values {
					if v == value {
						// 删除指定值
						idx.buckets[bucketIdx].entries[i].values = append(
							idx.buckets[bucketIdx].entries[i].values[:j],
							idx.buckets[bucketIdx].entries[i].values[j+1:]...)
						
						// 如果没有值了，删除整个条目
						if len(idx.buckets[bucketIdx].entries[i].values) == 0 {
							idx.buckets[bucketIdx].entries = append(
								idx.buckets[bucketIdx].entries[:i],
								idx.buckets[bucketIdx].entries[i+1:]...)
							
							// 更新计数
							idx.mutex.Lock()
							idx.count--
							idx.mutex.Unlock()
						}
						
						// 更新统计信息
						idx.stats.mutex.Lock()
						idx.stats.Deletes++
						idx.stats.mutex.Unlock()
						
						return nil
					}
				}
				return fmt.Errorf("value not found for key: %s", key)
			} else {
				// 删除整个键
				idx.buckets[bucketIdx].entries = append(
					idx.buckets[bucketIdx].entries[:i],
					idx.buckets[bucketIdx].entries[i+1:]...)
				
				// 更新计数
				idx.mutex.Lock()
				idx.count--
				idx.mutex.Unlock()
				
				// 更新
```
## 5. 优化哈希索引的扩容操作
```go
// resize 重新调整哈希表大小
func (idx *HashIndex) resize(newSize int) {
    startTime := time.Now()
    
    // 获取全局锁
    idx.mutex.Lock()
    defer idx.mutex.Unlock()
    
    // 检查是否已经被其他线程调整过大小
    if idx.size >= newSize {
        return
    }
    
    log.Printf("哈希索引开始扩容: %d -> %d", idx.size, newSize)
    
    // 创建新的桶数组
    newBuckets := make([]bucket, newSize)
    for i := range newBuckets {
        newBuckets[i] = bucket{entries: make([]entry, 0, 4)}
    }
    
    // 创建新的分片锁
    newBucketMutex := make([]sync.RWMutex, newSize)
    
    // 重新哈希所有条目
    for i := range idx.buckets {
        for _, e := range idx.buckets[i].entries {
            // 计算新的桶索引
            newBucketIdx := int(idx.hash(e.key) % uint32(newSize))
            
            // 添加到新桶
            newBuckets[newBucketIdx].entries = append(
                newBuckets[newBucketIdx].entries, e)
        }
    }
    
    // 更新索引状态
    idx.buckets = newBuckets
    idx.bucketMutex = newBucketMutex
    idx.size = newSize
    
    // 更新统计信息
    if idx.stats != nil {
        idx.stats.mutex.Lock()
        idx.stats.Resizes++
        idx.stats.LastResizeTime = time.Since(startTime)
        idx.stats.mutex.Unlock()
    }
    
    log.Printf("哈希索引扩容完成，耗时: %v", time.Since(startTime))
}

// optimizeBuckets 优化桶分布
func (idx *HashIndex) optimizeBuckets() {
    // 获取全局锁
    idx.mutex.RLock()
    
    // 计算桶的使用情况
    bucketStats := make([]int, idx.size)
    for i := range idx.buckets {
        bucketStats[i] = len(idx.buckets[i].entries)
    }
    idx.mutex.RUnlock()
    
    // 计算平均每个桶的条目数
    var total, max, empty int
    for _, count := range bucketStats {
        total += count
        if count > max {
            max = count
        }
        if count == 0 {
            empty++
        }
    }
    avg := float64(total) / float64(idx.size)
    
    // 计算标准差
    var variance float64
    for _, count := range bucketStats {
        diff := float64(count) - avg
        variance += diff * diff
    }
    stdDev := math.Sqrt(variance / float64(idx.size))
    
    // 记录统计信息
    if idx.stats != nil {
        idx.stats.mutex.Lock()
        idx.stats.AvgBucketSize = avg
        idx.stats.MaxBucketSize = max
        idx.stats.EmptyBuckets = empty
        idx.stats.BucketStdDev = stdDev
        idx.stats.mutex.Unlock()
    }
    
    // 如果标准差过大，考虑重新哈希
    if stdDev > avg*2 && idx.count > 1000 {
        log.Printf("哈希索引桶分布不均，考虑重新哈希: 平均=%v, 最大=%v, 空桶=%v, 标准差=%v",
            avg, max, empty, stdDev)
        
        // 异步执行重新哈希
        go idx.resize(idx.size)
    }
}