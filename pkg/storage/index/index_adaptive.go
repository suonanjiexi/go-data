package storage

import (
    "log"
    "sync"
    "time"
)

// AdaptiveIndexManager 自适应索引管理器
type AdaptiveIndexManager struct {
    db              *DB
    queryStats      map[string]*QueryStat // 查询统计信息
    statsMutex      sync.RWMutex
    indexManager    *IndexManager
    analyzeInterval time.Duration
    lastAnalyze     time.Time
    enabled         bool
}

// QueryStat 查询统计信息
type QueryStat struct {
    Table      string
    Columns    []string
    Count      int64
    TotalTime  time.Duration
    LastUsed   time.Time
    IsIndexed  bool
}

// NewAdaptiveIndexManager 创建自适应索引管理器
func NewAdaptiveIndexManager(db *DB, indexManager *IndexManager) *AdaptiveIndexManager {
    return &AdaptiveIndexManager{
        db:              db,
        queryStats:      make(map[string]*QueryStat),
        indexManager:    indexManager,
        analyzeInterval: 10 * time.Minute,
        lastAnalyze:     time.Now(),
        enabled:         true,
    }
}

// RecordQuery 记录查询统计
func (aim *AdaptiveIndexManager) RecordQuery(table string, columns []string, duration time.Duration) {
    if !aim.enabled {
        return
    }

    aim.statsMutex.Lock()
    defer aim.statsMutex.Unlock()

    // 生成查询键
    key := table
    for _, col := range columns {
        key += ":" + col
    }

    // 更新或创建统计信息
    stat, exists := aim.queryStats[key]
    if !exists {
        stat = &QueryStat{
            Table:     table,
            Columns:   columns,
            IsIndexed: false,
        }
        aim.queryStats[key] = stat
    }

    stat.Count++
    stat.TotalTime += duration
    stat.LastUsed = time.Now()
}

// AnalyzeAndOptimize 分析查询模式并优化索引
func (aim *AdaptiveIndexManager) AnalyzeAndOptimize() {
    if !aim.enabled || time.Since(aim.lastAnalyze) < aim.analyzeInterval {
        return
    }

    aim.statsMutex.Lock()
    defer aim.statsMutex.Unlock()

    aim.lastAnalyze = time.Now()
    log.Println("开始分析查询模式并优化索引...")

    // 找出频繁查询但未索引的列
    for key, stat := range aim.queryStats {
        // 如果查询次数超过阈值且未索引
        if stat.Count > 100 && !stat.IsIndexed {
            // 计算平均查询时间
            avgTime := stat.TotalTime / time.Duration(stat.Count)
            
            // 如果平均查询时间超过阈值，创建索引
            if avgTime > 5*time.Millisecond {
                log.Printf("为表 %s 的列 %v 创建索引（查询次数: %d, 平均时间: %v）", 
                    stat.Table, stat.Columns, stat.Count, avgTime)
                
                // 创建索引配置
                config := IndexConfig{
                    Table:   stat.Table,
                    Name:    "auto_idx_" + key,
                    Columns: stat.Columns,
                    Type:    BPlusTreeIndex,
                    Unique:  false,
                }
                
                // 创建索引
                if err := aim.indexManager.CreateIndex(config); err != nil {
                    log.Printf("创建自适应索引失败: %v", err)
                } else {
                    stat.IsIndexed = true
                }
            }
        }
    }

    // 找出不再使用的索引
    indexes := aim.indexManager.GetAllIndexes()
    for _, index := range indexes {
        // 如果索引名称以 "auto_idx_" 开头（自动创建的索引）
        if len(index.Config.Name) > 8 && index.Config.Name[:8] == "auto_idx_" {
            // 检查索引是否长时间未使用
            if index.LastAccess.IsZero() || time.Since(index.LastAccess) > 30*24*time.Hour {
                log.Printf("删除长时间未使用的索引: %s.%s", index.Config.Table, index.Config.Name)
                aim.indexManager.DropIndex(index.Config.Table, index.Config.Name)
            }
        }
    }

    log.Println("索引优化完成")
}

// StartMonitoring 开始监控查询模式
func (aim *AdaptiveIndexManager) StartMonitoring() {
    go func() {
        ticker := time.NewTicker(aim.analyzeInterval)
        defer ticker.Stop()

        for range ticker.C {
            aim.AnalyzeAndOptimize()
        }
    }()
    log.Println("自适应索引监控已启动")
}

// SetEnabled 启用或禁用自适应索引
func (aim *AdaptiveIndexManager) SetEnabled(enabled bool) {
    aim.statsMutex.Lock()
    defer aim.statsMutex.Unlock()
    aim.enabled = enabled
}