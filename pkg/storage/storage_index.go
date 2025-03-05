package storage

// GetIndexManager 返回数据库的索引管理器
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