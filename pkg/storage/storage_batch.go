package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// flushBatch 执行批量写入
func (db *DB) flushBatch() error {
	if len(db.batchBuffer) == 0 {
		return nil
	}

	// 使用写缓冲区
	db.writeBuffer.Reset()

	// 压缩数据
	var compressedWriter *gzip.Writer
	if db.compression {
		compressedWriter = gzip.NewWriter(db.writeBuffer)
	}

	// 批量处理写操作
	for _, op := range db.batchBuffer {
		// 如果启用了压缩
		if db.compression {
			if _, err := compressedWriter.Write(op.value); err != nil {
				return err
			}
		}

		// 更新内存数据
		db.data[op.key] = op.value

		// 更新缓存
		db.cache.Add(op.key, op.value)
	}

	// 如果启用了压缩，确保所有数据都被写入
	if db.compression {
		if err := compressedWriter.Close(); err != nil {
			return err
		}
	}

	// 清空批处理缓冲区
	db.batchBuffer = db.batchBuffer[:0]

	// 更新最后刷新时间
	db.lastFlushTime = time.Now()

	// 如果是持久化存储，写入文件
	if db.persistent {
		return db.persistToDisk()
	}

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

// persistToDisk 将数据持久化到磁盘
func (db *DB) persistToDisk() error {
	// 创建临时文件
	tmpFile := db.path + ".tmp"
	dir := filepath.Dir(db.path)

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建临时文件
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	// 使用缓冲写入提高性能
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024) // 4MB 缓冲区

	// 使用gob编码器
	var encoder *gob.Encoder

	// 如果启用了压缩
	if db.compression {
		compressedWriter, err := gzip.NewWriterLevel(bufWriter, gzip.BestSpeed) // 使用最快的压缩级别
		if err != nil {
			return fmt.Errorf("failed to create compressed writer: %w", err)
		}
		defer compressedWriter.Close()
		encoder = gob.NewEncoder(compressedWriter)
	} else {
		encoder = gob.NewEncoder(bufWriter)
	}

	// 编码数据
	if err := encoder.Encode(db.data); err != nil {
		return fmt.Errorf("failed to encode database data: %w", err)
	}

	// 刷新缓冲区
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
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

	return nil
}