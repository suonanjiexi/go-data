package cluster

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// NodeState 定义节点状态
type NodeState int

const (
	StateUnknown  NodeState = iota
	StateActive             // 活跃状态
	StateInactive           // 非活跃状态
	StateFailed             // 故障状态
)

// RaftState Raft节点状态
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID       string    `json:"id"`        // 节点唯一标识
	State    NodeState `json:"state"`     // 节点状态
	Address  string    `json:"address"`   // 节点地址
	Port     int       `json:"port"`      // 节点端口
	LastSeen time.Time `json:"last_seen"` // 最后一次心跳时间
	ShardIDs []int     `json:"shard_ids"` // 负责的分片ID列表
}

// Node 表示一个集群节点
type Node struct {
	info      NodeInfo
	mutex     sync.RWMutex
	listener  net.Listener
	peers     map[string]*NodeInfo // 其他节点信息
	peerMutex sync.RWMutex

	// Raft相关字段
	raftState     RaftState   // 当前Raft状态
	currentTerm   int64       // 当前任期
	votedFor      string      // 当前任期投票给谁
	leaderID      string      // 当前领导者ID
	electionTimer *time.Timer // 选举定时器
}

// NewNode 创建新节点
func NewNode(address string, port int) *Node {
	node := &Node{
		info: NodeInfo{
			ID:       fmt.Sprintf("%s:%d", address, port),
			State:    StateActive,
			Address:  address,
			Port:     port,
			LastSeen: time.Now(),
		},
		peers:       make(map[string]*NodeInfo),
		raftState:   Follower,
		currentTerm: 0,
	}

	// 初始化选举定时器
	node.resetElectionTimer()

	return node
}

// Start 启动节点服务
func (n *Node) Start() error {
	addr := fmt.Sprintf("%s:%d", n.info.Address, n.info.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	}
	n.listener = listener

	// 启动心跳检测
	go n.heartbeatLoop()
	// 启动节点发现
	go n.discoveryLoop()
	// 启动Raft定时器处理
	go n.raftLoop()

	return nil
}

// Stop 停止节点服务
func (n *Node) Stop() error {
	if n.listener != nil {
		return n.listener.Close()
	}
	return nil
}

// heartbeatLoop 心跳检测循环
func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.mutex.RLock()
		isLeader := n.raftState == Leader
		n.mutex.RUnlock()

		if isLeader {
			n.peerMutex.RLock()
			for _, peer := range n.peers {
				if err := n.sendHeartbeat(peer); err != nil {
					// 更新节点状态为失败
					n.updatePeerState(peer.ID, StateFailed)
				}
			}
			n.peerMutex.RUnlock()
		}
	}
}

// discoveryLoop 节点发现循环
func (n *Node) discoveryLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// 广播节点信息
		n.broadcastNodeInfo()
		// 清理失效节点
		n.cleanupFailedNodes()
	}
}

// raftLoop Raft状态机循环
func (n *Node) raftLoop() {
	for {
		select {
		case <-n.electionTimer.C:
			n.mutex.Lock()
			if n.raftState != Leader {
				// 转换为候选人状态
				n.raftState = Candidate
				n.currentTerm++
				n.votedFor = n.info.ID
				// 开始选举
				go n.startElection()
			}
			n.mutex.Unlock()
			// 重置选举定时器
			n.resetElectionTimer()
		}
	}
}

// resetElectionTimer 重置选举定时器
func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// 随机选举超时时间（150-300ms）
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.electionTimer = time.NewTimer(timeout)
}

// startElection 开始领导者选举
func (n *Node) startElection() {
	n.peerMutex.RLock()
	peers := make([]*NodeInfo, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.peerMutex.RUnlock()

	// 获取当前任期
	n.mutex.RLock()
	currentTerm := n.currentTerm
	n.mutex.RUnlock()

	// 统计选票
	votes := 1 // 给自己投票
	voteCh := make(chan bool, len(peers))

	// 并行请求投票
	for _, peer := range peers {
		go func(p *NodeInfo) {
			// 发送请求投票RPC
			// TODO: 实现请求投票RPC
			voteCh <- true // 临时模拟投票结果
		}(peer)
	}

	// 等待投票结果
	timeout := time.After(100 * time.Millisecond)
	for i := 0; i < len(peers); i++ {
		select {
		case vote := <-voteCh:
			if vote {
				votes++
				// 如果获得多数票，成为领导者
				if votes > (len(peers)+1)/2 {
					n.mutex.Lock()
					if n.raftState == Candidate && n.currentTerm == currentTerm {
						n.raftState = Leader
						n.leaderID = n.info.ID
					}
					n.mutex.Unlock()
					return
				}
			}
		case <-timeout:
			return
		}
	}
}

// sendHeartbeat 发送心跳包
func (n *Node) sendHeartbeat(peer *NodeInfo) error {
	// TODO: 实现心跳包发送逻辑
	return nil
}

// broadcastNodeInfo 广播节点信息
func (n *Node) broadcastNodeInfo() {
	// TODO: 实现节点信息广播逻辑
}

// cleanupFailedNodes 清理失效节点
func (n *Node) cleanupFailedNodes() {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()

	for id, peer := range n.peers {
		if time.Since(peer.LastSeen) > 1*time.Minute {
			delete(n.peers, id)
		}
	}
}

// updatePeerState 更新节点状态
func (n *Node) updatePeerState(peerID string, state NodeState) {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()

	if peer, exists := n.peers[peerID]; exists {
		peer.State = state
	}
}

// AddPeer 添加新的对等节点
func (n *Node) AddPeer(peer *NodeInfo) {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	n.peers[peer.ID] = peer
}

// RemovePeer 移除对等节点
func (n *Node) RemovePeer(peerID string) {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	delete(n.peers, peerID)
}

// GetPeers 获取所有对等节点信息
func (n *Node) GetPeers() []*NodeInfo {
	n.peerMutex.RLock()
	defer n.peerMutex.RUnlock()

	peers := make([]*NodeInfo, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	return peers
}
