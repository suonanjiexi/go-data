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