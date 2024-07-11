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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 存储包含自上次快照以来的所有稳定条目
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 已知已提交的最高的日志条目的索引     committedIndex
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经被应用到状态机的最高的日志条目的索引 appliedIndex
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// stabled 保存的是已经持久化到 storage 的 index
	stabled uint64

	// all entries that have not yet compact.
	// 保存所有尚未压缩的日志条目
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 保存任何传入的不稳定快照，在应用快照到状态机时使用。
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// newLog 函数的作用是使用给定的存储（storage）来返回一个新的 RaftLog 实例。它会将日志恢复到刚提交并应用最新快照的状态。(重启)
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	hardState, _, _ := storage.InitialState()
	entries, _ := storage.Entries(firstIndex, lastIndex + 1)

	raftLog := &RaftLog{
		storage: storage,
		committed: hardState.Commit,
		applied: firstIndex - 1,
		stabled: lastIndex,
		entries: entries,
		dummyIndex: firstIndex,
	}

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 我们需要在某个时间点压缩日志条目，例如存储压缩稳定日志条目阻止日志条目在内存中无限增长
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries返回所有未压缩的条目。
// 注意，从返回值中排除任何虚拟条目。
// 注意，这是您需要实现的测试存根函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
// 返回所有未持久化到 storage 的日志
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex() == l.stabled {
		return make([]pb.Entry, 0)
	}
	return l.entries[l.stabled + 1 - l.dummyIndex : l.LastIndex() + 1 - l.dummyIndex]
}

// nextEnts returns all the committed but not applied entries
// 返回所有已经提交但没有应用的日志
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed == l.applied {
		return make([]pb.Entry, 0)
	}
	return l.entries[l.applied + 1 - l.dummyIndex : l.committed + 1 - l.dummyIndex]
}

// LastIndex return the last index of the log entries
// 返回日志项的最后一个索引
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//if len(l.entries) == 0 {
	//	fmt.Println("in log.go LastIndex():len(l.entries) == 0")
	//	return 0
	//}
	return l.dummyIndex + uint64(len(l.entries)) - 1
}

// LastTerm 返回最后一条日志的任期
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		// fmt.Println("in log.go LastTerm():len(l.entries) == 0") // debug
		return 0
	}
	return l.entries[l.LastIndex() - l.dummyIndex].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//fmt.Println("log.go.Term used!") // debug
	//if i >= uint64(len(l.entries)) {
	//	return None,nil
	//}
	//fmt.Println(i,len(l.entries)) // debug
	if i >= l.dummyIndex {
		return l.entries[i - l.dummyIndex].Term, nil
	}
	return l.storage.Term(i)
}

// appendEntry 添加新的日志，并返回最后一条日志的索引
func (l *RaftLog) appendEntry(entries []*pb.Entry) uint64 {
	for idx := range entries {
		l.entries = append(l.entries,*entries[idx])
	}
	return l.LastIndex()
}

// handleCommit 处理日志是否需要提交，当该日志被大多数节点复制时可以提交，返回该日志是否提交
func (l *RaftLog) handleCommit(idx uint64,term uint64) bool {
	idxTerm, _ := l.Term(idx)
	if idx > l.committed && idxTerm == term {
		// 日志索引大于当前已提交的索引并且该日志是当前任期内创建的日志
		l.committed = idx
		return true
	}
	return false
}



