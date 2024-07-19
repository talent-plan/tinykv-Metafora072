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
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready 结构体用于保存已经处于 ready 状态的日志和消息,这些都是准备保存到持久化存储、提交或者发送给其他节点的
// Ready 中所有的字段都是只读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 表示节点当前的易失性状态，例如其角色（领导者、跟随者、候选人）。这种状态不需要持久化存储，如果没有更新则为 nil。
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 表示节点的持久状态，必须在发送任何消息之前保存到稳定存储中。包括当前任期、投票和提交索引等关键信息。
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 需要在发送任何消息之前保存到稳定存储中的日志条目列表。
	// 需要 持久化
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 指定要保存到稳定存储的快照。
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// 需要被输入到状态机中的日志，这些日志之前已经被保存到 Storage 中了
	// 需要 apply
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// 如果它包含MessageType_MsgSnapshot消息，则当接收到快照或调用ReportSnapshot失败时，应用程序必须向raft报告。
	// 在日志被写入到 Storage 之后需要发送的消息
	// 需要 发送
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
// RawNode 是对 Raft 的封装
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	preSoftState *SoftState   // 上一阶段的 Soft State
	preHardState pb.HardState // 上一阶段的 Hard State
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
// NewRawNode 根据配置和节点列表返回一个新的 RawNode
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rawNode := &RawNode{
		Raft:	newRaft(config),
	}
	rawNode.preHardState, _, _ = config.Storage.InitialState()
	rawNode.preSoftState = &SoftState{
		Lead:      rawNode.Raft.Lead,
		RaftState: rawNode.Raft.State,
	}
	return rawNode, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
// RawNode 转换到候选状态。
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
// Ready 返回 RawNode 的当前时间点状态。
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	ready := Ready {
		Messages:         rn.Raft.msgs,
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
	}

	if rn.isSoftStateUpdate() {
		ready.SoftState = &SoftState{
			Lead:      rn.Raft.Lead,
			RaftState: rn.Raft.State,
		}
	}
	if rn.isHardStateUpdate() {
		ready.HardState = pb.HardState{
			Commit: rn.Raft.RaftLog.committed,
			Vote:	rn.Raft.Vote,
			Term:	rn.Raft.Term,
		}
	}

	if IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) == false {
		ready.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}


	return ready
}

// isSoftStateUpdate 检查 SoftState 是否有更新
func (rn *RawNode) isSoftStateUpdate() bool {
	return rn.Raft.Lead != rn.preSoftState.Lead || rn.Raft.State != rn.preSoftState.RaftState
}


// isHardStateUpdate 检查 HardState 是否有更新
func (rn *RawNode) isHardStateUpdate() bool {
	return rn.Raft.Term != rn.preHardState.Term || rn.Raft.Vote != rn.preHardState.Vote || rn.Raft.RaftLog.committed != rn.preHardState.Commit
}


// HasReady called when RawNode user need to check if any Ready pending.
// 判断是否已经有同步完成并且需要上层处理的信息
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	hasReady := false

	// 有需要持久化的状态 检查 HardState 是否有更新
	if rn.isHardStateUpdate() {
		hasReady = true
	}

	// 检查 SoftState 是否有更新
	if rn.isSoftStateUpdate() {
		hasReady = true
	}

	// 有需要持久化的条目
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 {
		hasReady = true
	}
	// 有需要应用的条目
	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		hasReady = true
	}
	// 有需要发送的 Message
	if len(rn.Raft.msgs) > 0 {
		hasReady = true
	}

	if IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) == false {
		hasReady = true
	}

	return hasReady
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// Advance 通知 RawNode，应用程序已经应用并保存了最后一个 Ready 结果中的进度。
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).

	// 上次执行 Ready 更新了 softState
	if rd.SoftState != nil {
		rn.preSoftState = rd.SoftState
	}

	// 检查 HardState 是否是默认值，默认值说明没有更新，此时不应该更新 preHardState
	if IsEmptyHardState(rd.HardState) == false {
		rn.preHardState = rd.HardState
	}

	// 更新 RaftLog 状态
	rn.Raft.RaftLog.stabled += uint64(len(rd.Entries))
	rn.Raft.RaftLog.applied += uint64(len(rd.CommittedEntries))
	//if len(rd.Entries) > 0 {
	//	rn.Raft.RaftLog.stabled += uint64(len(rd.Entries))
	//}
	//if len(rd.CommittedEntries) > 0 {
	//	rn.Raft.RaftLog.applied += uint64(len(rd.CommittedEntries))
	//}
	rn.Raft.msgs = nil
	// Snap
	rn.Raft.RaftLog.maybeCompact()        //丢弃被压缩的暂存日志；

	rn.Raft.RaftLog.pendingSnapshot = nil //清空 pendingSnapshot；



}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
