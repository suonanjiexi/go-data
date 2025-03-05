package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// RecoverFromBackup 从备份文件恢复数据库
func (db *DB) RecoverFromBackup(backupPath string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 打开备份文件
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// 检查文件是否为gzip压缩
	var reader io.Reader = file
	bufReader := bufio.NewReader(file)
	
	// 读取前几个字节检查是否为gzip格式
	header, err := bufReader.Peek(2)
	if err != nil {
		return fmt.Errorf("failed to read file header: %w", err)
	}
	
	// gzip文件的魔数是0x1f 0x8b
	if header[0] == 0x1f && header[1] == 0x8b {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufReader
	}

	// 使用gob解码数据
	decoder := gob.NewDecoder(reader)
	
	// 创建临时数据存储
	tempData := make(map[string][]byte)
	
	if err := decoder.Decode(&tempData); err != nil {
		return fmt.Errorf("failed to decode backup data: %w", err)
	}

	// 替换当前数据
	db.data = tempData
	
	// 清空并重建缓存
	db.cache.Purge()
	for k, v := range db.data {
		db.cache.Add(k, v)
	}

	// 重建索引
	if err := db.indexManager.LoadIndexes(); err != nil {
		return fmt.Errorf("failed to rebuild indexes: %w", err)
	}

	return nil
}

// CreateBackup 创建数据库备份
func (db *DB) CreateBackup(backupPath string) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 确保目录存在
	dir := filepath.Dir(backupPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建备份文件
	file, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// 使用缓冲写入
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)

	// 如果启用了压缩
	var encoder *gob.Encoder
	if db.compression {
		compressedWriter, err := gzip.NewWriterLevel(bufWriter, gzip.BestCompression)
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

	// 更新最后备份时间
	db.lastBackupTime = time.Now()

	return nil
}