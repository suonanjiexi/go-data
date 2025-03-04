package handler

import (
    "encoding/json"
    "net/http"
    "sync"
    "time"

    "github.com/gorilla/mux"
)

type ClusterStatus struct {
    Nodes []NodeStatus `json:"nodes"`
    Shards []ShardStatus `json:"shards"`
    LeaderID string `json:"leader_id"`
    HealthScore float64 `json:"health_score"`
}

type NodeStatus struct {
    ID string `json:"id"`
    Address string `json:"address"`
    Role string `json:"role"`
    Status string `json:"status"`
    LastHeartbeat time.Time `json:"last_heartbeat"`
    Performance PerformanceMetrics `json:"performance"`
}

type ShardStatus struct {
    ID string `json:"id"`
    NodeID string `json:"node_id"`
    DataSize int64 `json:"data_size"`
    RecordCount int64 `json:"record_count"`
}

type PerformanceMetrics struct {
    QPS float64 `json:"qps"`
    Latency float64 `json:"latency"`
    CPUUsage float64 `json:"cpu_usage"`
    MemoryUsage float64 `json:"memory_usage"`
    DiskIO float64 `json:"disk_io"`
}

type AdminHandler struct {
    clusterManager *cluster.Manager
    metricsCollector *metrics.Collector
    mu sync.RWMutex
}

func NewAdminHandler(cm *cluster.Manager, mc *metrics.Collector) *AdminHandler {
    return &AdminHandler{
        clusterManager: cm,
        metricsCollector: mc,
    }
}

func (h *AdminHandler) RegisterRoutes(r *mux.Router) {
    r.HandleFunc("/api/cluster/status", h.handleClusterStatus).Methods("GET")
    r.HandleFunc("/api/cluster/config", h.handleUpdateConfig).Methods("POST")
    r.HandleFunc("/api/metrics", h.handleMetrics).Methods("GET")
}

func (h *AdminHandler) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
    h.mu.RLock()
    defer h.mu.RUnlock()

    status := h.clusterManager.GetStatus()
    json.NewEncoder(w).Encode(status)
}

func (h *AdminHandler) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
    var config map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    if err := h.clusterManager.UpdateConfig(config); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
}

func (h *AdminHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
    metrics := h.metricsCollector.GetMetrics()
    json.NewEncoder(w).Encode(metrics)
}