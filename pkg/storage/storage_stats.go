package storage

import (
	"time"
)

// GetStats 获取数据库统计信息
func (db *DB) GetStats() DBStats {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	stats := DBStats{
		KeyCount:       len(db.data),
		IndexCount:     len(db.indexManager.indexes),
		StartTime:      db.startTime,
		Uptime:         time.Since(db.startTime),
		CacheHitRate:   0,
		CacheHits:      db.cacheHits,
		CacheMisses:    db.cacheMisses,
		GetOps:         db.getOps,
		PutOps:         db.putOps,
		DeleteOps:      db.deleteOps,
		AvgGetTime:     db.avgGetTime,
		AvgPutTime:     db.avgPutTime,
		LastFlushTime:  db.lastFlushTime,
		LastBackupTime: db.lastBackupTime,
	}

	// 计算总数据大小
	var totalSize int64
	for _, v := range db.data {
		totalSize += int64(len(v))
	}
	stats.TotalDataSize = totalSize

	// 计算缓存命中率
	if db.cacheHits+db.cacheMisses > 0 {
		stats.CacheHitRate = float64(db.cacheHits) / float64(db.cacheHits+db.cacheMisses)
	}

	return stats
}

// ResetStats 重置数据库统计信息
func (db *DB) ResetStats() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.cacheHits = 0
	db.cacheMisses = 0
	db.getOps = 0
	db.putOps = 0
	db.deleteOps = 0
	db.avgGetTime = 0
	db.avgPutTime = 0
}