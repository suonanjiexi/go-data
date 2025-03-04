package metrics

import (
    "sync"
    "time"
    "runtime"
)

type Collector struct {
    metrics map[string]*NodeMetrics
    mu sync.RWMutex
    updateInterval time.Duration
}

type NodeMetrics struct {
    QPS float64
    Latency float64
    CPUUsage float64
    MemoryUsage float64
    DiskIO float64
    LastUpdate time.Time
    requestCount uint64
    totalLatency time.Duration
}

func NewCollector(updateInterval time.Duration) *Collector {
    c := &Collector{
        metrics: make(map[string]*NodeMetrics),
        updateInterval: updateInterval,
    }
    go c.periodicUpdate()
    return c
}

func (c *Collector) periodicUpdate() {
    ticker := time.NewTicker(c.updateInterval)
    for range ticker.C {
        c.updateMetrics()
    }
}

func (c *Collector) updateMetrics() {
    c.mu.Lock()
    defer c.mu.Unlock()

    for _, metrics := range c.metrics {
        // 更新QPS
        duration := time.Since(metrics.LastUpdate)
        metrics.QPS = float64(metrics.requestCount) / duration.Seconds()
        metrics.requestCount = 0

        // 更新平均延迟
        if metrics.requestCount > 0 {
            metrics.Latency = float64(metrics.totalLatency) / float64(metrics.requestCount) / float64(time.Millisecond)
        }
        metrics.totalLatency = 0

        // 更新资源使用情况
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        metrics.MemoryUsage = float64(m.Alloc) / float64(m.Sys)

        metrics.LastUpdate = time.Now()
    }
}

func (c *Collector) RecordRequest(nodeID string, latency time.Duration) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if _, exists := c.metrics[nodeID]; !exists {
        c.metrics[nodeID] = &NodeMetrics{
            LastUpdate: time.Now(),
        }
    }

    metrics := c.metrics[nodeID]
    metrics.requestCount++
    metrics.totalLatency += latency
}

func (c *Collector) GetMetrics() map[string]*NodeMetrics {
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 创建指标数据的副本
    result := make(map[string]*NodeMetrics)
    for nodeID, metrics := range c.metrics {
        result[nodeID] = &NodeMetrics{
            QPS: metrics.QPS,
            Latency: metrics.Latency,
            CPUUsage: metrics.CPUUsage,
            MemoryUsage: metrics.MemoryUsage,
            DiskIO: metrics.DiskIO,
            LastUpdate: metrics.LastUpdate,
        }
    }

    return result
}

func (c *Collector) UpdateResourceUsage(nodeID string, cpu float64, disk float64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if metrics, exists := c.metrics[nodeID]; exists {
        metrics.CPUUsage = cpu
        metrics.DiskIO = disk
    }
}