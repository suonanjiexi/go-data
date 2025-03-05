package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// NewIndexManager 创建一个新的索引管理器
func NewIndexManager(db *DB) *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
		db:      db,
	}
}

// CreateIndex 创建一个新索引
func (im *IndexManager) CreateIndex(config IndexConfig) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 验证索引配置
	if config.Name == "" || config.Table == "" || len(config.Columns) == 0 {
		return ErrInvalidIndex
	}

	// 检查索引类型是否有效
	if config.Type != BPlusTreeIndex && config.Type != HashIndex {
		return ErrInvalidIndexType
	}

	// 检查索引是否已存在
	indexKey := fmt.Sprintf("%s:%s", config.Table, config.Name)
	if _, exists := im.indexes[indexKey]; exists {
		return ErrIndexExists
	}

	// 创建新索引
	index := &Index{
		Config: config,
		Root:   nil,
		Degree: 4, // 默认B+树度为4
	}

	// 初始化节点缓存
	nodeCache, err := lru.New[string, *BPlusTreeNode](1000) // 默认缓存1000个节点
	if err != nil {
		return fmt.Errorf("failed to create node cache: %w", err)
	}
	index.NodeCache = nodeCache

	// 初始化统计信息
	index.Stats = &IndexStats{}

	// 初始化最后访问时间
	index.LastAccess = time.Now()

	// 初始化B+树根节点
	if config.Type == BPlusTreeIndex {
		index.Root = &BPlusTreeNode{
			IsLeaf: true,
			Keys:   make([]string, 0),
			Values: make([][]string, 0),
		}
	}

	// 将索引添加到管理器
	im.indexes[indexKey] = index

	// 构建索引
	err = im.buildIndex(index)
	if err != nil {
		delete(im.indexes, indexKey)
		return err
	}

	// 持久化索引元数据
	return im.saveIndexMetadata(index)
}

// buildIndex 为现有数据构建索引
func (im *IndexManager) buildIndex(index *Index) error {
	// 获取表中所有记录
	// 这里应该实现遍历表中所有记录并构建索引的逻辑
	// 简化实现，实际应该使用迭代器或范围查询
	// TODO: 实现完整的索引构建逻辑
	return nil
}

// saveIndexMetadata 保存索引元数据
func (im *IndexManager) saveIndexMetadata(index *Index) error {
	// 构建索引元数据键
	metaKey := fmt.Sprintf("index:%s:%s", index.Config.Table, index.Config.Name)

	// 序列化索引配置
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(index.Config); err != nil {
		return err
	}

	// 保存到数据库
	return im.db.Put(metaKey, buf.Bytes())
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(table, name string) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 检查索引是否存在
	if _, exists := im.indexes[indexKey]; !exists {
		return ErrIndexNotFound
	}

	// 从管理器中删除索引
	delete(im.indexes, indexKey)

	// 删除索引元数据
	metaKey := fmt.Sprintf("index:%s:%s", table, name)
	return im.db.Delete(metaKey)
}

// GetIndex 通过表名和索引名获取索引
func (im *IndexManager) GetIndex(table, name string) (*Index, error) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 查找索引
	index, exists := im.indexes[indexKey]
	if !exists {
		return nil, ErrIndexNotFound
	}

	// 更新最后访问时间
	index.LastAccess = time.Now()

	return index, nil
}

// GetTableIndex 通过表名和列名获取索引
func (im *IndexManager) GetTableIndex(table, column string) *Index {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 遍历所有索引，查找包含指定列的索引
	for _, index := range im.indexes {
		if index.Config.Table == table {
			// 检查索引是否包含指定列
			for _, col := range index.Config.Columns {
				if col == column {
					// 更新最后访问时间
					index.LastAccess = time.Now()
					return index
				}
			}
		}
	}

	return nil
}

// 添加索引缓存，提高查询性能
type indexCacheKey struct {
	table  string
	column string
}

// 添加索引缓存字段到 IndexManager
type IndexManager struct {
	indexes     map[string]*Index // 索引名称 -> 索引
	db          *DB               // 数据库引用
	mutex       sync.RWMutex      // 读写锁
	columnCache sync.Map          // 表列 -> 索引的缓存
}

// 优化 GetTableIndex 方法，使用缓存
func (im *IndexManager) GetTableIndex(table, column string) *Index {
	// 先检查缓存
	cacheKey := indexCacheKey{table: table, column: column}
	if cachedIndex, ok := im.columnCache.Load(cacheKey); ok {
		index := cachedIndex.(*Index)
		// 更新最后访问时间
		index.LastAccess = time.Now()
		return index
	}

	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 遍历所有索引，查找包含指定列的索引
	for _, index := range im.indexes {
		if index.Config.Table == table {
			// 检查索引是否包含指定列
			for _, col := range index.Config.Columns {
				if col == column {
					// 更新最后访问时间
					index.LastAccess = time.Now()
					// 添加到缓存
					im.columnCache.Store(cacheKey, index)
					return index
				}
			}
		}
	}

	return nil
}

// LoadIndexes 从数据库加载所有索引
func (im *IndexManager) LoadIndexes() error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 清空现有索引
	im.indexes = make(map[string]*Index)

	// TODO: 实现从数据库加载索引元数据的逻辑
	// 这需要遍历所有以"index:"开头的键，解码索引配置，并重建索引

	return nil
}

// GetIndexManager 获取数据库的索引管理器
func (db *DB) GetIndexManager() *IndexManager {
	return db.indexManager
}