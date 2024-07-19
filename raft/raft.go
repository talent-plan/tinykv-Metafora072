// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	//"fmt"
	rand2 "math/rand"
	"sort"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
// Config 结构体包含了启动 Raft 节点所需的参数
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// 本地 Raft 节点的唯一标识符，不能为 0
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// 包含 Raft 集群中所有节点（包括自身）的 ID。仅在启动新的 Raft 集群时设置。如果在从先前配置重新启动 Raft 时设置了 peers，将会引发 panic。peers 目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// 在选举之间必须经过的 Node.Tick 调用次数。如果一个跟随者在 ElectionTick 时间内没有收到当前任期领导者的任何消息，它将成为候选人并开始选举。
	// ElectionTick 必须大于 HeartbeatTick。
	// 建议 ElectionTick = 10 * HeartbeatTick 以避免不必要的领导者切换。
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// 在心跳之间必须经过的 Node.Tick 调用次数。领导者每 HeartbeatTick 个 tick 发送一次心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// Raft 的存储。Raft 生成的日志条目和状态将存储在 Storage 中。Raft 在需要时从 Storage 中读取持久化的条目和状态，并在重新启动时从 Storage 中读取先前的状态和配置。
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// 最后应用的日志条目索引。仅在重新启动 Raft 时设置。Raft 不会返回小于或等于 Applied 的条目给应用程序。
	// 如果在重新启动时未设置 Applied，Raft 可能会返回先前已应用的条目。这是一个非常依赖于应用程序的配置。
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// handleUpdate 返回是否更新了 Progress 信息
func (pr *Progress) handleUpdate(idx uint64) bool {
	isUpdate := false
	if pr.Match < idx {
		pr.Match = idx
		pr.Next = idx + 1
		isUpdate = true
	}
	return isUpdate
}

type Raft struct {
	// 节点的唯一标识符
	id uint64

	// 当前任期号
	Term uint64
	// 当前任期内投票给的候选人ID
	Vote uint64

	// the log
	// 指向日志结构的指针，存储所有的日志条目。
	RaftLog *RaftLog

	// log replication progress of each peers
	// 每个节点的日志复制进度，键是节点ID，值是进度信息。
	Prs map[uint64]*Progress

	// this peer's role
	// 当前节点的角色状态（如领导者、候选人、跟随者）。
	State StateType

	// votes records
	// 记录当前任期内收到的投票情况，键是节点ID，值是是否投票。
	votes map[uint64]bool

	// msgs need to send
	// 需要发送的消息列表。
	msgs []pb.Message

	// the leader id
	// 当前领导者的ID。
	Lead uint64

	// heartbeat interval, should send
	// 心跳间隔时间，领导者定期发送心跳。
	heartbeatTimeout int
	// baseline of election interval
	// 选举超时时间的基准值。
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 自上次心跳超时以来经过的时间，仅领导者维护。
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 自上次选举超时或收到当前领导者的有效消息以来经过的时间。
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// 领导者转移目标的ID，当其值不为零时表示正在进行领导者转移。
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 当前待处理的配置变更的日志索引，仅允许一个配置变更待处理。
	PendingConfIndex uint64

	// 随机超时选举，[electionTimeout, 2*electionTimeout)[150ms,300ms]
	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).，
	hardState, conState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = conState.Nodes
	}
	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,    // Term 和 Vote 从持久化存储中读取
		Vote:             hardState.Vote,    // Term 和 Vote 从持久化存储中读取
		RaftLog:          newLog(c.Storage), // Log 也从持久化存储中读取
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	// 生成随机选举超时时间，范围在 [r.electionTimeout, 2*r.electionTimeout]
	source := rand2.NewSource(time.Now().UnixNano())
	rng := rand2.New(source)
	raft.randomElectionTimeout = raft.electionTimeout + rng.Intn(raft.electionTimeout)

	// 更新集群配置
	raft.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		raft.Prs[id] = &Progress{}
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 发送附加日志条目（如果有的话）和当前的提交索引给指定的节点。如果发送了消息，则返回 true。
func (r *Raft) sendAppend(to uint64) bool {
	//fmt.Println("sendAppend used!") // debug
	// Your Code Here (2A).
	preLogIndex := r.Prs[to].Next - 1
	//fmt.Println("sendAppend used Term!")
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err == nil {
		// 发送 Entries 数组给 followers
		appendMessage := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:		 to,
			From:	 r.id,
			Term:	 r.Term,
			LogTerm: preLogTerm,
			Index:   preLogIndex,
			Entries: make([]*pb.Entry, 0),
			Commit:  r.RaftLog.committed,
		}
		// 需要追加到 follower 上的日志集合
		nextEntries := r.RaftLog.entries[preLogIndex + 1 - r.RaftLog.dummyIndex : r.RaftLog.LastIndex() + 1 - r.RaftLog.dummyIndex]
		for idx := range nextEntries {
			appendMessage.Entries = append(appendMessage.Entries, &nextEntries[idx])
		}

		r.msgs = append(r.msgs, appendMessage)
		return true
	}
	// 当 Leader append 日志给落后 node 节点时，发现对方所需要的 entry 已经被 compact。此时 Leader 会发送 Snapshot 过去。
	r.sendSnapshot(to)
	return false
}

// sendSnapshot 发送快照给其它节点(to)
func (r *Raft) sendSnapshot(to uint64) {
	// Snapshot 返回最新的快照。
	// 如果快照暂时不可用，它应该返回 ErrSnapshotTemporarilyUnavailable，
	// 这样 Raft 状态机就能知道存储需要一些时间来准备快照，并稍后调用 Snapshot。
	// Snapshot() (pb.Snapshot, error)

	// 节点生成快照
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		//因为 Snapshot 很大，不会马上生成，这里为了避免阻塞，如果 Snapshot 还没有生成好，Snapshot 会先返回 raft.ErrSnapshotTemporarilyUnavailable 错误
		// Leader 就应该放弃本次 Snapshot，等待下一次再次请求 Snapshot。
		return
	}

	message := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:		  to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, message)

	// 更新目标节点的 Next 索引
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// 向给定的节点发送心跳包
func (r *Raft) sendHeartbeat(to uint64) {
	//fmt.Println("sendHeartbeat used!") // debug
	// Your Code Here (2A).
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:		 to,
		From:	 r.id,
		Term:	 r.Term,
	}
	r.msgs = append(r.msgs, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			// 超时，发起选举
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			// 超时，发起选举
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// leader该发送心跳包
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:		 r.id,
				From:    r.id,
			})
		}
	}
}

//var followernum uint64 = 0
//var leadernum uint64 = 0

// becomeFollower transform this peer's state to Follower
// 将此节点的状态转换为 Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	//if r.State == StateLeader {
	//	leadernum--
	//}
	//fmt.Println("becomeFollower used!") // debug
	// Your Code Here (2A).
	if term > r.Term {
		r.Vote = None
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0

	// 生成随机选举超时时间，范围在 [r.electionTimeout, 2*r.electionTimeout]
	source := rand2.NewSource(time.Now().UnixNano())
	rng := rand2.New(source)
	r.randomElectionTimeout = r.electionTimeout + rng.Intn(r.electionTimeout)

}

// becomeCandidate transform this peer's state to candidate
// 将此节点的状态转换为 Candidate
func (r *Raft) becomeCandidate() {
	//if r.State == StateLeader {
	//	leadernum--
	//}
	//fmt.Println("becomeCandidate used!") // debug
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Vote = r.id
	r.votes[r.id] = true

	r.electionElapsed = 0

	// 生成随机选举超时时间，范围在 [r.electionTimeout, 2*r.electionTimeout]
	source := rand2.NewSource(time.Now().UnixNano())
	rng := rand2.New(source)
	r.randomElectionTimeout = r.electionTimeout + rng.Intn(r.electionTimeout)

}

// becomeLeader transform this peer's state to leader
// 将此节点的状态转换为 Leader
func (r *Raft) becomeLeader() {
	//if r.State != StateLeader {
	//	leadernum++
	//}
	//fmt.Println("LeaderNum: ", leadernum)
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//fmt.Println("becomeLeader used!") // debug
	r.State = StateLeader
	r.Lead = r.id
	//初始化 nextIndex 和 matchIndex
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 // 初始化为 leader 的最后一条日志索引（后续出现冲突会往前移动）
		r.Prs[id].Match = 0                        // 初始化为 0 就可以了
	}

	// Leader should propose a noop entry on its term ??
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 该函数接收一个 Msg，然后根据节点的角色和 Msg 的类型调用不同的处理函数。
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 请求节点开始选举(发起投票)
			r.handleStartElection(m)
		case pb.MessageType_MsgAppend:
			// Leader 给其他节点同步日志条目
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// Candidate 请求投票
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			// Leader 发送的心跳
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			// Leader 将快照发送给其他节点
			r.handleSnapshot(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 请求节点开始选举(发起投票)
			r.handleStartElection(m)
		case pb.MessageType_MsgAppend:
			// Leader 给其他节点同步日志条目
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			// Candidate 请求投票
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			// 节点告诉 Candidate 投票结果
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			// Leader 发送的心跳
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			// Leader 将快照发送给其他节点
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			// Leader 发送心跳
			r.broadcastHeartBeat()
		case pb.MessageType_MsgPropose:
			// 上层请求 propose 条目。
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			// Leader 给其他节点同步日志条目
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			// 节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgRequestVote:
			// Candidate 请求投票
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			// 节点对心跳的回应
			r.handleHeartBeatResponse(m)
		}
	}
	return nil
}

// 成为候选者，请求节点开始选举(发起投票)
func (r *Raft) handleStartElection(m pb.Message) {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}


	//向所有节点发起 RequestVote 请求
	for requestId := range r.Prs {
		if requestId == r.id {
			continue
		}
		message := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      requestId,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		}
		// message.LogTerm
		//if len(r.RaftLog.entries) == 0 {
		//	message.LogTerm = 0
		//} else {
		//	message.LogTerm = r.RaftLog.entries[r.RaftLog.LastIndex() - r.RaftLog.dummyIndex].Term
		//}

		r.msgs = append(r.msgs, message)

	}

	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // 给自己投票
}

// 节点收到 RequestVote 请求时候的处理
func (r *Raft) handleRequestVote(m pb.Message) {
	//fmt.Println("handleRequestVote used!") // debug
	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:	 r.id,
		Term:    r.Term,
		//Reject:  false,
	}
	// 投票条件
	// 1. Candidate 任期大于自己并且日志足够新
	// 2. Candidate 任期和自己相等并且自己在当前任期内没有投过票或者已经投给了 Candidate，并且 Candidate 的日志足够新
	if m.Term > r.Term {
		r.becomeFollower(m.Term,None)
	}
	if ( (m.Term > r.Term) ||  (m.Term == r.Term && (r.Vote == None || r.Vote == m.From)) ) && ( m.LogTerm > r.RaftLog.LastTerm() || m.LogTerm == r. RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex() )  {
		r.becomeFollower(m.Term,None)
		r.Vote = m.From
	} else {
		// 拒绝投票
		message.Reject = true
	}

	r.msgs = append(r.msgs, message)

}

// handleRequestVoteResponse 节点收到 RequestVoteResponse 时候的处理
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	//fmt.Println("handleRequestVoteResponse used!") // debug
	// 投的赞成票
	if m.Reject == false {
		r.votes[m.From] = true
	} else {
		r.votes[m.From] = false
	}

	voteCount := 0 // 记录收集了多少赞成票

	for _, legal := range r.votes {
		if legal {
			voteCount++
		}
	}

	// 要求的大多数赞成票数量
	legalCount := len(r.Prs) / 2 + 1

	if m.Reject == false {
		// 半数以上节点投票给自己，可以成为 leader
		if voteCount >= legalCount {
			r.becomeLeader()
		}
	} else {
		if r.Term < m.Term { // 小于拒绝者的任期，直接 Candidate -> Follower
			r.becomeFollower(m.Term, None)
		}
		if len(r.votes) - voteCount >= legalCount { // 半数以上节点拒绝投票给自己，直接 Candidate -> Follower
			r.becomeFollower(r.Term, None)
		}
	}
}

// broadcastHeartBeat 节点广播心跳包
func (r *Raft) broadcastHeartBeat() {
	for goatId := range r.Prs {
		if goatId == r.id {
			continue
		}
		r.sendHeartbeat(goatId)
	}
	r.heartbeatElapsed = 0
}

// handleHeartBeatResponse 领导者节点对心跳的回应
func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	if m.Reject == true {
		// 心跳包被拒绝，说明对方的任期更高
		r.becomeFollower(m.Term, None)
	} else {
		// 心跳包被接受
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() { // 对方日志和自己不同步
			// 向对方同步日志
			r.sendAppend(m.From)
		}
	}
}

// handlePropose Leader 追加从上层应用接收到的新日志，并广播给 Follower
func (r *Raft) handlePropose(m pb.Message) {
	for idx := range m.Entries {
		// 设置新的日志的索引和日期
		m.Entries[idx].Term = r.Term
		m.Entries[idx].Index = r.RaftLog.LastIndex() + 1 + uint64(idx)
	}
	r.RaftLog.appendEntry(m.Entries)

	// 更新节点日志复制进度 Progress
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		// 广播日志同步
		for goatId := range r.Prs {
			if goatId == r.id {
				continue
			}
			r.sendAppend(goatId)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
// 处理同步日志条目
func (r *Raft) handleAppendEntries(m pb.Message) {
	//fmt.Println("handleAppendEntries used!") // debug
	// Your Code Here (2A).
	responseMessage := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:		 m.From,
		From:	 r.id,
		Term: 	 r.Term,
		Reject:  true,
	}

	// 对方的任期更小，拒绝其同步请求
	if m.Term < r.Term {
		r.msgs = append(r.msgs, responseMessage)
		return
	}

	preLogIndex := m.Index
	preLogTerm := m.LogTerm
	r.becomeFollower(m.Term,m.From)

	//fmt.Println("handleAppendEntries used Term!")
	//var preTerm uint64 = 0
	//if preLogIndex <= r.RaftLog.LastIndex() {
	//	preTerm, _ = r.RaftLog.Term(preLogIndex)
	//}

	// 检验之前同步的日志是否冲突，已经同步的日志和对方并不匹配(Index不一样或者Term不一样)
	if preLogIndex > r.RaftLog.LastIndex() {
		responseMessage.Index = r.RaftLog.LastIndex()
	} else {
		preTerm, _ := r.RaftLog.Term(preLogIndex)
		if preTerm != preLogTerm {
			responseMessage.Index = r.RaftLog.LastIndex()
			for _, ent := range r.RaftLog.entries {
				if ent.Term == preTerm {
					//找到冲突任期的上一个任期的idx位置
					responseMessage.Index = ent.Index - 1
					break
				}
			}
		}
		if preLogTerm == preTerm {
			if len(m.Entries) > 0 {
				idx := m.Index + 1
				lenEntries := len(m.Entries)
				//isConflict := false
				for ; idx < r.RaftLog.LastIndex() && idx <= m.Entries[lenEntries-1].Index; idx++ {
					//fmt.Println("Line644 used Term!") // debug
					curTerm, _ := r.RaftLog.Term(idx)
					if curTerm != m.Entries[idx-m.Index-1].Term {
						//isConflict = true
						break
					}
				}

				if idx - m.Index - 1 != uint64(len(m.Entries)) {
					// 当前条目在idx位置出现冲突

					// 冲突位置后面的/条目均舍弃
					if len(r.RaftLog.entries) > 0 {
						r.RaftLog.entries = r.RaftLog.entries[:idx-r.RaftLog.dummyIndex]
					}
					// 强行与对方(Leader)同步，也就是强行添加idx之后的日志
					r.RaftLog.appendEntry(m.Entries[idx-m.Index-1:])
					r.RaftLog.stabled = min(r.RaftLog.stabled, idx-1)
				}
			}

			// 更新commitIndex
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(m.Entries)))
			}

			//同意同步请求
			responseMessage.Reject = false
			responseMessage.Index = m.Index + uint64(len(m.Entries))
			responseMessage.LogTerm, _ = r.RaftLog.Term(responseMessage.Index)
		}
	}

	r.msgs = append(r.msgs, responseMessage)

}

// handleAppendEntriesResponse Leader节点收到 AppendEntriesResponse 的处理
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// fmt.Println("handleAppendEntriesResponse used!") // debug
	if m.Reject == true { // 拒绝同步
		if m.Term > r.Term { // 对方任期大于自己，自己无法成为Leader了
			r.becomeFollower(m.Term, None)
		} else {
			// preLog 日志冲突，根据返回的Index重新尝试同步
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	// 同意同步
	if r.Prs[m.From].handleUpdate(m.Index) {
		// 更新了 Progress
		if r.handleCommit(){ // 处理可能会发生的日志提交
			// 发生日志提交了，则要广播给所有的 Follower 也提交该日志
			for goatId := range r.Prs {
				if goatId == r.id {
					continue
				}
				r.sendAppend(goatId)
			}
		}
	}
}

// handleCommit 处理可能会发生的日志提交，返回
func (r *Raft) handleCommit() bool {
	matchArr := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArr = append(matchArr, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArr))
	midIdx := len(r.Prs) / 2 + 1
	canCommitIndex := matchArr[midIdx - 1]
	// 处理提交 canCommitIndex
	return r.RaftLog.handleCommit(canCommitIndex, r.Term)
}

// handleHeartbeat handle Heartbeat RPC request
// 处理心跳包请求
func (r *Raft) handleHeartbeat(m pb.Message) {
	// fmt.Println("handleHeartbeat used!") // debug
	// Your Code Here (2A).
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:		 m.From,
		From:	 r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Reject:  false,
	}

	if r.Term > m.Term {
		// 任期比自己小的心跳包请求，直接拒绝
		message.Reject = true
	} else {
		// 成为发送心跳包的跟随者，信任其是 Leader
		r.becomeFollower(m.Term, m.From)
	}

	r.msgs = append(r.msgs, message)
}


// handleSnapshot handle Snapshot RPC request
// handleSnapshot 处理发来的快照请求
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	responseMessage := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:	 r.id,
		Term:	 r.Term,
	}

	// 如果对方 term 小于自身的 term 直接拒绝这次快照的数据
	if m.Term < r.Term {
		responseMessage.Reject = true
		r.msgs = append(r.msgs, responseMessage)
		return
	}
	// 如果已经提交的日志大于等于快照中的日志，也需要拒绝这次快照
	if r.RaftLog.committed >= m.Snapshot.Metadata.Index {
		responseMessage.Reject = true
		r.msgs = append(r.msgs, responseMessage)
		return
	}

	r.becomeFollower(m.Term, m.From)
	// 更新日志数据
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.RaftLog.dummyIndex = m.Snapshot.Metadata.Index + 1
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.entries = make([]pb.Entry, 0)
	// 更新集群配置
	r.Prs = make(map[uint64]*Progress)
	for _, nodeId := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[nodeId] = &Progress{
			Next:	r.RaftLog.LastIndex() + 1,
		}
	}
	responseMessage.Index = m.Snapshot.Metadata.Index
	responseMessage.Reject = false

	r.msgs = append(r.msgs, responseMessage)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
