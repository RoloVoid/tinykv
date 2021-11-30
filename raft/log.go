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
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

func errHandler(err error) {
	if err != nil {
		panic(err)
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstindex, err := storage.FirstIndex()
	errHandler(err) //no need to intend to repair
	snapshot, err := storage.Snapshot()
	errHandler(err)
	// entries, err := storage.Entries()
	errHandler(err)
	//initialize, all the pointers are at the startline
	NewRaftLog := &RaftLog{
		storage:   storage,
		committed: firstindex - 1,
		applied:   firstindex - 1,
		stabled:   firstindex - 1,
		// entries:         entries,
		pendingSnapshot: &snapshot,
	}
	return NewRaftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	lastindex, err := l.storage.LastIndex()
	errHandler(err)
	hi := lastindex + 1
	lo := l.stabled + 1
	data, err := l.storage.Entries(lo, hi)
	errHandler(err)
	return data
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied >= l.committed {
		return nil
	}
	data, err := l.storage.Entries(l.applied+1, l.committed)
	errHandler(err)
	return data
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	li, err := l.storage.LastIndex()
	errHandler(err)
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	term, err := l.storage.Term(i)
	return term, err
}

// A method used to check out whether there is unstable snapshot
func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && !IsEmptySnap(l.pendingSnapshot)
}
