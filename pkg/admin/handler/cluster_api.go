package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/suonanjiexi/cyber-db/pkg/cluster"
)

// ClusterAPIHandler 处理集群管理相关的API请求
type ClusterAPIHandler struct {
	clusterManager *cluster.Manager
}

// NewClusterAPIHandler 创建新的集群API处理器
func NewClusterAPIHandler(cm *cluster.Manager) *ClusterAPIHandler {
	return &ClusterAPIHandler{
		clusterManager: cm,
	}
}

// RegisterRoutes 注册路由
func (h *ClusterAPIHandler) RegisterRoutes(r *mux.Router) {
	// 节点管理
	r.HandleFunc("/api/cluster/nodes", h.handleGetNodes).Methods("GET")
	r.HandleFunc("/api/cluster/nodes", h.handleAddNode).Methods("POST")
	r.HandleFunc("/api/cluster/nodes/{id}", h.handleGetNode).Methods("GET")
	r.HandleFunc("/api/cluster/nodes/{id}/decommission", h.handleDecommissionNode).Methods("POST")
	r.HandleFunc("/api/cluster/nodes/{id}", h.handleRemoveNode).Methods("DELETE")

	// 分片管理
	r.HandleFunc("/api/cluster/shards", h.handleGetShards).Methods("GET")
	r.HandleFunc("/api/cluster/shards/{id}", h.handleGetShard).Methods("GET")
	r.HandleFunc("/api/cluster/shards/rebalance", h.handleRebalanceShards).Methods("POST")
	r.HandleFunc("/api/cluster/shards/{id}/migrate", h.handleMigrateShard).Methods("POST")

	// 复制管理
	r.HandleFunc("/api/cluster/replication/status", h.handleReplicationStatus).Methods("GET")
	r.HandleFunc("/api/cluster/replication/config", h.handleUpdateReplicationConfig).Methods("POST")
}

// 节点管理API

// handleGetNodes 获取所有节点信息
func (h *ClusterAPIHandler) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	nodes := h.clusterManager.GetNodes()
	json.NewEncoder(w).Encode(nodes)
}

// NodeAddRequest 添加节点请求
type NodeAddRequest struct {
	Address string   `json:"address"`
	Tags    []string `json:"tags"`
	Weight  int      `json:"weight"`
}

// handleAddNode 添加新节点
func (h *ClusterAPIHandler) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var req NodeAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	node := &cluster.ClusterNode{
		Address:  req.Address,
		Status:   cluster.NodeStatusJoining,
		JoinTime: time.Now(),
		LastSeen: time.Now(),
	}

	if err := h.clusterManager.AddNode(node); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(node)
}

// handleGetNode 获取单个节点信息
func (h *ClusterAPIHandler) handleGetNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	node, err := h.clusterManager.GetNode(nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(node)
}

// handleDecommissionNode 下线节点
func (h *ClusterAPIHandler) handleDecommissionNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	if err := h.clusterManager.DecommissionNode(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleRemoveNode 删除节点
func (h *ClusterAPIHandler) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	if err := h.clusterManager.RemoveNode(nodeID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// 分片管理API

// handleGetShards 获取所有分片信息
func (h *ClusterAPIHandler) handleGetShards(w http.ResponseWriter, r *http.Request) {
	shards := h.clusterManager.GetShards()
	json.NewEncoder(w).Encode(shards)
}

// handleGetShard 获取单个分片信息
func (h *ClusterAPIHandler) handleGetShard(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shardID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid shard ID", http.StatusBadRequest)
		return
	}

	shard, err := h.clusterManager.GetShard(shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(shard)
}

// handleRebalanceShards 重新平衡分片
func (h *ClusterAPIHandler) handleRebalanceShards(w http.ResponseWriter, r *http.Request) {
	if err := h.clusterManager.RebalanceShards(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// ShardMigrateRequest 分片迁移请求
type ShardMigrateRequest struct {
	ShardID    int    `json:"shard_id"`
	SourceNode string `json:"source_node"`
	TargetNode string `json:"target_node"`
}

// handleMigrateShard 迁移分片
func (h *ClusterAPIHandler) handleMigrateShard(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shardID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid shard ID", http.StatusBadRequest)
		return
	}

	var req ShardMigrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.clusterManager.MigrateShard(shardID, req.TargetNode); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// 复制管理API

// ReplicationStatus 复制状态
type ReplicationStatus struct {
	Enabled      bool    `json:"enabled"`
	ReplicaCount int     `json:"replica_count"`
	SyncRatio    float64 `json:"sync_ratio"` // 已同步的副本比例
	Replicas     []struct {
		NodeID string `json:"node_id"`
		Status string `json:"status"`
		Lag    int64  `json:"lag"` // 复制延迟（毫秒）
	} `json:"replicas"`
}

// handleReplicationStatus 获取复制状态
func (h *ClusterAPIHandler) handleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	status := h.clusterManager.GetReplicationStatus()
	json.NewEncoder(w).Encode(status)
}

// ReplicationConfigRequest 复制配置请求
type ReplicationConfigRequest struct {
	Enabled      bool `json:"enabled"`
	ReplicaCount int  `json:"replica_count"`
}

// handleUpdateReplicationConfig 更新复制配置
func (h *ClusterAPIHandler) handleUpdateReplicationConfig(w http.ResponseWriter, r *http.Request) {
	var req ReplicationConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.clusterManager.UpdateReplicationConfig(req.Enabled, req.ReplicaCount); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}