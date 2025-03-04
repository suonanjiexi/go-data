package cluster

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// ShardStatus 分片状态
const (
	ShardStatusNormal    = "normal"
	ShardStatusRebalancing = "rebalancing"
	ShardStatusMigrating = "migrating"
)

// Shard 表示一个数据分片
type Shard struct {
	ID          int       // 分片ID
	Status      string    // 分片状态
	PrimaryNode string    // 主节点ID
	ReplicaNodes []string // 副本节点ID列表
	KeyRange    [2]string // 键范围
	CreateTime  time.Time // 创建时间
	UpdateTime  time.Time // 更新时间
	Size        int64     // 分片大小（字节）
	RecordCount int64     // 记录数量
}

// ShardManager 分片管理器
type ShardManager struct {
	shards       map[int]*Shard
	nodeShards   map[string][]int // 节点到分片的映射
	mutex        sync.RWMutex
	hashFunction func(key string) int // 哈希函数
	shardCount   int                  // 分片总数
	replicaCount int                  // 副本数量
}

// NewShardManager 创建新的分片管理器
func NewShardManager(shardCount, replicaCount int) *ShardManager {
	sm := &ShardManager{
		shards:       make(map[int]*Shard),
		nodeShards:   make(map[string][]int),
		shardCount:   shardCount,
		replicaCount: replicaCount,
		hashFunction: defaultHashFunction,
	}

	return sm
}

// defaultHashFunction 默认哈希函数
func defaultHashFunction(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

// InitShards 初始化分片
func (sm *ShardManager) InitShards(nodes []string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for sharding")
	}

	// 清空现有分片
	sm.shards = make(map[int]*Shard)
	sm.nodeShards = make(map[string][]int)

	// 初始化每个节点的分片列表
	for _, nodeID := range nodes {
		sm.nodeShards[nodeID] = make([]int, 0)
	}

	// 创建分片并分配给节点
	for i := 0; i < sm.shardCount; i++ {
		// 选择主节点（简单轮询）
		primaryNode := nodes[i%len(nodes)]
		
		// 选择副本节点
		replicaNodes := make([]string, 0, sm.replicaCount)
		for j := 1; j <= sm.replicaCount; j++ {
			if len(nodes) > 1 {
				replicaNodeIndex := (i + j) % len(nodes)
				if nodes[replicaNodeIndex] != primaryNode {
					replicaNodes = append(replicaNodes, nodes[replicaNodeIndex])
				}
			}
		}

		//
	}

	return nil
}

// ShardState 定义分片状态
type ShardState int

const (
	ShardUnknown     ShardState = iota
	ShardActive                 // 活跃状态
	ShardMigrating              // 迁移中
	ShardUnavailable            // 不可用
)

// ShardInfo 分片信息
type ShardInfo struct {
	ID         int        // 分片ID
	State      ShardState // 分片状态
	LeaderNode string     // 领导者节点ID
	Followers  []string   // 跟随者节点列表
	DataRange  [2]uint64  // 数据范围（哈希值范围）
	CreateTime time.Time  // 创建时间
	UpdateTime time.Time  // 更新时间
	Version    int64      // 版本号，用于Raft一致性检查
}

// ShardManager 分片管理器
type ShardManager struct {
	mutex      sync.RWMutex
	shards     map[int]*ShardInfo // 分片信息
	nodeShards map[string][]int   // 节点负责的分片
	node       *Node              // 所属节点
	replicator *Replicator        // 数据复制器

	// Raft相关字段
	logEntries  []*LogEntry // 分片操作日志
	commitIndex int64       // 已提交的最高日志索引
	lastApplied int64       // 已应用到状态机的最高日志索引
}

// NewShardManager 创建分片管理器
func NewShardManager(node *Node) *ShardManager {
	sm := &ShardManager{
		shards:      make(map[int]*ShardInfo),
		nodeShards:  make(map[string][]int),
		node:        node,
		logEntries:  make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
	}
	sm.replicator = NewReplicator(sm)
	return sm
}

// CreateShard 创建新分片
func (sm *ShardManager) CreateShard(id int, dataRange [2]uint64) error {
	// 只有Leader节点可以创建分片
	if sm.node.raftState != Leader {
		return errors.New("not leader")
	}

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if _, exists := sm.shards[id]; exists {
		return errors.New("shard already exists")
	}

	// 创建分片信息
	shard := &ShardInfo{
		ID:         id,
		State:      ShardActive,
		LeaderNode: sm.node.info.ID,
		DataRange:  dataRange,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
		Version:    1,
	}

	// 创建日志条目
	entry := &LogEntry{
		Term:  sm.node.currentTerm,
		Index: int64(len(sm.logEntries) + 1),
		Command: map[string]interface{}{
			"type":       "create_shard",
			"shard_id":   id,
			"data_range": dataRange,
		},
	}

	// 追加日志并复制到其他节点
	sm.logEntries = append(sm.logEntries, entry)
	sm.replicateLog(entry)

	// 更新状态
	sm.shards[id] = shard
	sm.nodeShards[sm.node.info.ID] = append(sm.nodeShards[sm.node.info.ID], id)
	return nil
}

// replicateLog 复制日志到其他节点
func (sm *ShardManager) replicateLog(entry *LogEntry) {
	for _, peer := range sm.node.GetPeers() {
		go func(p *NodeInfo) {
			// 发送AppendEntries RPC
			sm.sendAppendEntries(p.ID, entry)
		}(peer)
	}
}

// sendAppendEntries 发送追加日志RPC
func (sm *ShardManager) sendAppendEntries(nodeID string, entry *LogEntry) error {
	// TODO: 实现发送AppendEntries RPC的逻辑
	return nil
}

// GetShardLocation 获取数据所属的分片
func (sm *ShardManager) GetShardLocation(key string) (int, error) {
	// 计算键的哈希值
	hash := fnv.New64()
	hash.Write([]byte(key))
	hashValue := hash.Sum64()

	// 查找对应的分片
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	for id, shard := range sm.shards {
		if hashValue >= shard.DataRange[0] && hashValue < shard.DataRange[1] {
			return id, nil
		}
	}

	return 0, errors.New("no shard found for key")
}

// HandleNodeFailure 处理节点故障
func (sm *ShardManager) HandleNodeFailure(nodeID string) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// 获取故障节点负责的分片
	shardIDs, exists := sm.nodeShards[nodeID]
	if !exists {
		return nil
	}

	// 处理每个受影响的分片
	for _, shardID := range shardIDs {
		shard := sm.shards[shardID]

		// 如果是领导者节点故障，需要触发新的领导者选举
		if shard.LeaderNode == nodeID {
			if len(shard.Followers) > 0 {
				// 创建日志条目记录领导者变更
				entry := &LogEntry{
					Term:  sm.node.currentTerm,
					Index: int64(len(sm.logEntries) + 1),
					Command: map[string]interface{}{
						"type":       "leader_change",
						"shard_id":   shardID,
						"old_leader": nodeID,
						"new_leader": shard.Followers[0],
					},
				}

				// 追加日志并复制
				sm.logEntries = append(sm.logEntries, entry)
				sm.replicateLog(entry)

				// 更新分片信息
				shard.LeaderNode = shard.Followers[0]
				shard.Followers = shard.Followers[1:]
				shard.UpdateTime = time.Now()
				shard.Version++
			} else {
				// 没有可用的跟随者节点，标记分片为不可用
				shard.State = ShardUnavailable
			}
		} else {
			// 跟随者节点故障，从列表中移除
			for i, follower := range shard.Followers {
				if follower == nodeID {
					shard.Followers = append(shard.Followers[:i], shard.Followers[i+1:]...)
					break
				}
			}
		}
	}

	// 清理节点信息
	delete(sm.nodeShards, nodeID)
	return nil
}

// Replicator 数据复制器
type Replicator struct {
	manager    *ShardManager
	mutex      sync.RWMutex
	replicaMap map[string]bool // 记录正在复制的分片
}

// NewReplicator 创建数据复制器
func NewReplicator(manager *ShardManager) *Replicator {
	return &Replicator{
		manager:    manager,
		replicaMap: make(map[string]bool),
	}
}

// StartReplication 开始数据复制
func (r *Replicator) StartReplication(shardID int, targetNode string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// 生成复制任务ID
	taskID := fmt.Sprintf("%d-%s", shardID, targetNode)

	// 检查是否已经在复制
	if r.replicaMap[taskID] {
		return errors.New("replication already in progress")
	}

	// 标记开始复制
	r.replicaMap[taskID] = true

	// 异步执行复制
	go func() {
		defer func() {
			r.mutex.Lock()
			delete(r.replicaMap, taskID)
			r.mutex.Unlock()
		}()

		// 实现数据复制逻辑
		// TODO: 实现具体的数据复制机制
	}()

	return nil
}
