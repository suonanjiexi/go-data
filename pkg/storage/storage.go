package storage

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义错误类型
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBNotOpen   = errors.New("database not open")
)

// DB 表示一个简单的键值存储数据库
type DB struct {
	path          string                     // 数据库文件路径
	isOpen        bool                       // 数据库是否打开
	data          map[string][]byte          // 内存中的数据
	cache         *lru.Cache[string, []byte] // LRU缓存
	mutex         sync.RWMutex               // 读写锁，保证并发安全
	persistent    bool                       // 是否持久化存储
	counters      map[string]int             // 自增ID计数器
	counterMutex  sync.RWMutex               // 计数器的互斥锁
	charset       string                     // 字符集编码，默认为utf8mb4
	indexManager  *IndexManager              // 索引管理器
	batchSize     int                        // 批量写入大小
	batchBuffer   []writeOp                  // 批量写入缓冲区
	batchMutex    sync.Mutex                 // 批量写入互斥锁
	flushTicker   *time.Ticker               // 定期刷新计时器
	flushDone     chan struct{}              // 用于停止刷新goroutine的通道
	preReadSize   int                        // 预读取大小
	preReadBuffer *sync.Map                  // 预读取缓冲区
	writeBuffer   *bytes.Buffer              // 写入缓冲区
	compression   bool                       // 是否启用压缩
	memoryPool    sync.Pool                  // 内存池
	shardCount    int                        // 分片数量，用于细粒度锁控制
	shardMutexes  []sync.RWMutex             // 分片锁数组
	resultCache   *lru.Cache[string, []byte] // 查询结果缓存
}

// writeOp 表示一个写操作
type writeOp struct {
	key   string
	value []byte
}

// NewDB 创建一个新的数据库实例
func NewDB(path string, persistent bool) *DB {
	// 增大缓存容量，提高缓存命中率
	cache, _ := lru.New[string, []byte](100000)
	// 创建查询结果缓存
	resultCache, _ := lru.New[string, []byte](10000)

	// 创建分片锁，提高并发性能
	shardCount := 32 // 使用32个分片锁
	shardMutexes := make([]sync.RWMutex, shardCount)

	db := &DB{
		path:          path,
		isOpen:        false,
		data:          make(map[string][]byte),
		cache:         cache,
		resultCache:   resultCache,
		persistent:    persistent,
		counters:      make(map[string]int),
		charset:       "utf8mb4",
		batchSize:     10000,                                  // 增大批量写入大小
		batchBuffer:   make([]writeOp, 0, 10000),              // 预分配容量
		flushTicker:   time.NewTicker(500 * time.Millisecond), // 更频繁地刷新
		flushDone:     make(chan struct{}),
		preReadSize:   2 * 1024 * 1024, // 增大预读取大小
		preReadBuffer: &sync.Map{},
		writeBuffer:   bytes.NewBuffer(make([]byte, 0, 1024*1024)), // 预分配容量
		compression:   true,
		shardCount:    shardCount,
		shardMutexes:  shardMutexes,
		memoryPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096) // 增大缓冲区大小
			},
		},
	}
	// 创建索引管理器
	db.indexManager = NewIndexManager(db)

	// 启动后台刷新goroutine
	go db.backgroundFlush()

	return db
}

// GetNextID 获取指定表的下一个自增ID
func (db *DB) GetNextID(table string) int {
	db.counterMutex.Lock()
	defer db.counterMutex.Unlock()

	// 获取当前计数器值并增加
	currentID := db.counters[table]
	currentID++

	// 更新计数器
	db.counters[table] = currentID

	return currentID
}

// Open 打开数据库
func (db *DB) Open() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.isOpen {
		return nil
	}

	// 如果是持久化数据库，尝试从文件加载数据
	if db.persistent {
		// 确保目录存在
		dir := filepath.Dir(db.path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// 尝试读取现有数据文件
		if _, err := os.Stat(db.path); err == nil {
			file, err := os.Open(db.path)
			if err != nil {
				return fmt.Errorf("failed to open database file: %w", err)
			}
			defer file.Close()

			// 使用gob解码数据
			decoder := gob.NewDecoder(file)
			if err := decoder.Decode(&db.data); err != nil {
				return fmt.Errorf("failed to decode database data: %w", err)
			}

			// 预热缓存，提高初始性能
			count := 0
			for k, v := range db.data {
				if count < 10000 { // 限制预热数量
					db.cache.Add(k, v)
					count++
				} else {
					break
				}
			}
		}
	}

	db.isOpen = true

	// 加载索引
	if err := db.indexManager.LoadIndexes(); err != nil {
		return fmt.Errorf("failed to load indexes: %w", err)
	}

	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return nil
	}

	// 停止后台刷新goroutine
	if db.flushTicker != nil {
		db.flushTicker.Stop()
		close(db.flushDone)
	}

	// 刷新所有未写入的数据
	db.flushBatch()

	// 如果是持久化数据库，将数据写入文件
	if db.persistent {
		// 创建临时文件，成功后再替换原文件，避免写入过程中的数据损坏
		tmpFile := db.path + ".tmp"

		// 确保目录存在
		dir := filepath.Dir(db.path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// 创建临时文件
		file, err := os.Create(tmpFile)
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		defer file.Close()

		// 使用gob编码数据
		encoder := gob.NewEncoder(file)
		if err := encoder.Encode(db.data); err != nil {
			return fmt.Errorf("failed to encode database data: %w", err)
		}

		// 确保数据写入磁盘
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}

		// 关闭文件
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}

		// 替换原文件
		if err := os.Rename(tmpFile, db.path); err != nil {
			return fmt.Errorf("failed to rename temp file: %w", err)
		}
	}

	// 清理资源
	db.cache = nil
	db.resultCache = nil
	db.preReadBuffer = nil

	db.isOpen = false
	return nil
}

// Get 获取键对应的值
func (db *DB) Get(key string) ([]byte, error) {
	// 首先检查缓存
	if db.cache != nil {
		if value, ok := db.cache.Get(key); ok {
			// 缓存命中，直接返回
			result := make([]byte, len(value.([]byte)))
			copy(result, value.([]byte))
			return result, nil
		}
	}

	// 检查预读缓存
	if db.preReadBuffer != nil {
		if value, ok := db.preReadBuffer.Load(key); ok {
			// 预读缓存命中
			result := make([]byte, len(value.([]byte)))
			copy(result, value.([]byte))
			// 添加到主缓存
			if db.cache != nil {
				db.cache.Add(key, value)
			}
			return result, nil
		}
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	value, ok := db.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// 返回值的副本，避免外部修改影响内部数据
	result := make([]byte, len(value))
	copy(result, value)

	// 添加到缓存
	if db.cache != nil {
		db.cache.Add(key, value)
	}

	// 执行预读
	go db.preReadNearbyKeys(key)

	return result, nil
}

// preReadNearbyKeys 预读取附近的键值对
func (db *DB) preReadNearbyKeys(key string) {
	if db.preReadBuffer == nil || db.preReadSize <= 0 {
		return
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return
	}

	// 简单实现：预读取字典序相近的键
	count := 0
	size := 0
	for k, v := range db.data {
		// 跳过已经读取的键
		if k == key {
			continue
		}

		// 只预读取字典序相近的键
		if len(k) > 0 && len(key) > 0 && k[0] == key[0] {
			db.preReadBuffer.Store(k, v)
			count++
			size += len(v)

			// 限制预读数量和大小
			if count >= 10 || size >= db.preReadSize {
				break
			}
		}
	}
}

// Put 存储键值对
func (db *DB) Put(key string, value []byte) error {
	// 验证字符串是否为有效的UTF-8MB4编码
	if !utf8.Valid(value) {
		return errors.New("invalid UTF-8 encoding")
	}

	// 使用批量写入缓冲区
	db.batchMutex.Lock()
	defer db.batchMutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 存储值的副本，避免外部修改影响内部数据
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// 添加到批量写入缓冲区
	db.batchBuffer = append(db.batchBuffer, writeOp{key: key, value: valueCopy})

	// 如果缓冲区达到批量写入大小，执行批量写入
	if len(db.batchBuffer) >= db.batchSize {
		return db.flushBatch()
	}

	return nil
}

// flushBatch 将批量写入缓冲区的数据写入存储
func (db *DB) flushBatch() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 将缓冲区中的所有操作应用到数据存储
	for _, op := range db.batchBuffer {
		db.data[op.key] = op.value

		// 更新缓存
		if db.cache != nil {
			db.cache.Add(op.key, op.value)
		}
	}

	// 清空缓冲区
	db.batchBuffer = make([]writeOp, 0, db.batchSize) // 预分配容量以减少内存分配

	return nil
}

// backgroundFlush 后台定期刷新批量写入缓冲区
func (db *DB) backgroundFlush() {
	for {
		select {
		case <-db.flushTicker.C:
			// 获取批量写入锁
			db.batchMutex.Lock()

			// 如果缓冲区有数据，执行刷新
			if len(db.batchBuffer) > 0 {
				db.flushBatch()
			}

			db.batchMutex.Unlock()
		case <-db.flushDone:
			return // 退出goroutine
		}
	}
}

// Delete 删除键值对
func (db *DB) Delete(key string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	delete(db.data, key)
	return nil
}

// Has 检查键是否存在
func (db *DB) Has(key string) (bool, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return false, ErrDBNotOpen
	}

	_, ok := db.data[key]
	return ok, nil
}

// GetIndexManager 获取索引管理器
func (db *DB) GetIndexManager() *IndexManager {
	return db.indexManager
}

// CreateIndex 创建索引
func (db *DB) CreateIndex(table, name string, columns []string, indexType int, unique bool) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	config := IndexConfig{
		Name:    name,
		Table:   table,
		Columns: columns,
		Type:    indexType,
		Unique:  unique,
	}

	return db.indexManager.CreateIndex(config)
}

// DropIndex 删除索引
func (db *DB) DropIndex(table, name string) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	return db.indexManager.DropIndex(table, name)
}
