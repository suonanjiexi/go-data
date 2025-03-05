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
    tableName := index.Config.Table
    
    // 创建表扫描迭代器
    iter, err := im.db.NewTableIterator(tableName)
    if err != nil {
        return fmt.Errorf("创建表迭代器失败: %w", err)
    }
    defer iter.Close()
    
    // 批量处理，提高性能
    const batchSize = 1000
    batch := make([]struct {
        key   string
        value string
    }, 0, batchSize)
    
    // 遍历表中的所有记录
    for iter.Next() {
        record := iter.Record()
        
        // 提取索引键
        indexKey, err := im.extractIndexKey(record, index.Config.Columns)
        if err != nil {
            log.Printf("警告: 从记录中提取索引键失败: %v", err)
            continue
        }
        
        // 添加到批处理
        batch = append(batch, struct {
            key   string
            value string
        }{
            key:   indexKey,
            value: record.ID,
        })
        
        // 批量处理
        if len(batch) >= batchSize {
            if err := im.processBatch(index, batch); err != nil {
                return fmt.Errorf("批量处理索引数据失败: %w", err)
            }
            batch = batch[:0] // 清空批处理
        }
    }
    
    // 处理剩余的批次
    if len(batch) > 0 {
        if err := im.processBatch(index, batch); err != nil {
            return fmt.Errorf("批量处理索引数据失败: %w", err)
        }
    }
    
    log.Printf("成功为表 %s 构建索引 %s，共索引 %d 条记录", 
        index.Config.Table, index.Config.Name, index.Size)
    
    return nil
}

// processBatch 批量处理索引数据
func (im *IndexManager) processBatch(index *Index, batch []struct {
    key   string
    value string
}) error {
    // 根据索引类型选择不同的处理方式
    switch index.Config.Type {
    case BPlusTreeIndex:
        // 对于B+树索引，直接插入
        for _, item := range batch {
            if err := index.Insert(item.key, item.value); err != nil {
                // 唯一索引冲突可以忽略
                if !strings.Contains(err.Error(), "唯一索引冲突") {
                    return err
                }
            }
        }
    case HashIndex:
        // 对于哈希索引，使用哈希索引的插入方法
        hashIndex, ok := index.HashIndex.(*HashIndex)
        if !ok {
            return fmt.Errorf("无效的哈希索引实例")
        }
        
        for _, item := range batch {
            if err := hashIndex.Insert(item.key, item.value); err != nil {
                // 唯一索引冲突可以忽略
                if !strings.Contains(err.Error(), "duplicate key") {
                    return err
                }
            }
        }
    default:
        return fmt.Errorf("不支持的索引类型: %d", index.Config.Type)
    }
    
    return nil
}

// extractIndexKey 从记录中提取索引键
func (im *IndexManager) extractIndexKey(record *Record, columns []string) (string, error) {
    if len(columns) == 1 {
        // 单列索引
        value, ok := record.Data[columns[0]]
        if !ok {
            return "", fmt.Errorf("记录中不存在列 %s", columns[0])
        }
        
        // 将值转换为字符串
        switch v := value.(type) {
        case string:
            return v, nil
        case int, int32, int64, float32, float64, bool:
            return fmt.Sprintf("%v", v), nil
        default:
            return "", fmt.Errorf("不支持的索引列类型: %T", v)
        }
    } else {
        // 复合索引
        var keyParts []string
        for _, col := range columns {
            value, ok := record.Data[col]
            if !ok {
                return "", fmt.Errorf("记录中不存在列 %s", col)
            }
            
            // 将值转换为字符串
            switch v := value.(type) {
            case string:
                keyParts = append(keyParts, v)
            case int, int32, int64, float32, float64, bool:
                keyParts = append(keyParts, fmt.Sprintf("%v", v))
            default:
                return "", fmt.Errorf("不支持的索引列类型: %T", v)
            }
        }
        
        // 使用特殊分隔符连接复合键
        return strings.Join(keyParts, "\x00"), nil
    }
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

    // 使用前缀扫描查找所有索引元数据
    prefix := "index:"
    keys, err := im.db.PrefixScan(prefix)
    if err != nil {
        return fmt.Errorf("扫描索引元数据失败: %w", err)
    }

    // 并行加载索引以提高性能
    var wg sync.WaitGroup
    indexChan := make(chan *Index, len(keys))
    errChan := make(chan error, len(keys))

    for _, key := range keys {
        wg.Add(1)
        go func(metaKey string) {
            defer wg.Done()

            // 获取索引元数据
            data, err := im.db.Get(metaKey)
            if err != nil {
                errChan <- fmt.Errorf("获取索引元数据失败 %s: %w", metaKey, err)
                return
            }

            // 解码索引配置
            var config IndexConfig
            buf := bytes.NewBuffer(data)
            dec := gob.NewDecoder(buf)
            if err := dec.Decode(&config); err != nil {
                errChan <- fmt.Errorf("解码索引配置失败 %s: %w", metaKey, err)
                return
            }

            // 创建索引实例
            index := &Index{
                Config: config,
                Degree: 4, // 默认B+树度为4
            }

            // 初始化节点缓存
            nodeCache, err := lru.New[string, *BPlusTreeNode](1000)
            if err != nil {
                errChan <- fmt.Errorf("创建节点缓存失败: %w", err)
                return
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
            } else if config.Type == HashIndex {
                // 初始化哈希索引
                // 这里应该根据实际情况初始化哈希索引
            }

            // 将索引添加到通道
            indexChan <- index
        }(key)
    }

    // 等待所有索引加载完成
    wg.Wait()
    close(indexChan)
    close(errChan)

    // 检查是否有错误
    for err := range errChan {
        return err
    }

    // 将加载的索引添加到管理器
    for index := range indexChan {
        indexKey := fmt.Sprintf("%s:%s", index.Config.Table, index.Config.Name)
        im.indexes[indexKey] = index
    }

    log.Printf("成功加载 %d 个索引", len(im.indexes))
    return nil
}

// GetIndexManager 获取数据库的索引管理器
func (db *DB) GetIndexManager() *IndexManager {
	return db.indexManager
}

// GetAllIndexes 获取所有索引
func (im *IndexManager) GetAllIndexes() []*Index {
    im.mutex.RLock()
    defer im.mutex.RUnlock()
    
    indexes := make([]*Index, 0, len(im.indexes))
    for _, index := range im.indexes {
        indexes = append(indexes, index)
    }
    
    return indexes
}