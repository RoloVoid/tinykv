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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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

	// rolo: the next three become unstable part
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	// rolo: unstable
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// rolo unstable
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// a counter to check entry
	committing map[uint64]*commitMsg

	// log from config
	logger Logger
}

// one simple struct to check if committed
type commitMsg struct {
	counter   int
	committed bool
}

func errHandler(err error) {
	if err != nil {
		log.Errorf(err.Error())
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	li, _ := storage.LastIndex()
	// errHandler(err)
	fi, _ := storage.FirstIndex()
	// errHandler(err)
	entries, _ := storage.Entries(fi, li+1)
	// errHandler(err)
	hd, _, _ := storage.InitialState()
	// errHandler(err)
	// initialize, all the pointers are at the startline
	NewRaftLog := &RaftLog{
		storage:    storage,
		committed:  hd.Commit,
		applied:    fi - 1,
		stabled:    li, // is not actually stabled, real stabled is li,which means stabled)
		entries:    entries,
		committing: make(map[uint64]*commitMsg),
	}
	return NewRaftLog
}

// ---> add one more logger
func newLogWithLogger(storage Storage, logger Logger) *RaftLog {
	temp := newLog(storage)
	temp.logger = logger
	return temp
}

// ---> check and reset applied
func (l *RaftLog) setApplied(logApplied uint64) {
	// of course not,just in case
	if logApplied == 0 {
		return
	}
	// ---> if applied is stale, just acounce it
	if l.committed < logApplied || logApplied < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", logApplied, l.applied, l.committed)
		return
	}
	l.applied = logApplied
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	truncatedIndex, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		if truncatedIndex > firstIndex {
			l.entries = l.entries[truncatedIndex-firstIndex:]
		}
	}
}

func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.stabled+1-l.FirstIndex():]
}

// nextEnts returns all the committed but not applied entries
// rolo:it is used for build ready
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if l.committed-l.FirstIndex()+1 < 0 || l.applied-l.FirstIndex()+1 > l.LastIndex() {
			return nil
		}

		if l.applied-l.FirstIndex()+1 >= 0 && l.committed-l.FirstIndex()+1 <= uint64(len(l.entries)) {
			// doubt
			// fmt.Println("ents index", l.applied-l.FirstIndex()+1, l.committed-l.FirstIndex()+1)
			// fmt.Println("ents", l.entries, l.entries[l.applied-l.FirstIndex()+1:l.committed-l.FirstIndex()+1])
			return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
		}
	}
	return nil
}

// LastIndex returns the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// if no record,get it from storage
	if len(l.entries) == 0 {
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

// FirstIndex returns the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i - 1
	}
	return l.entries[0].Index
}

// Term returns the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if i >= l.FirstIndex() {
			index := i - l.FirstIndex()
			if index >= uint64(len(l.entries)) {
				return 0, ErrUnavailable
			}
			return l.entries[index].Term, nil
		}
	}
	// if not, get it from storage
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

// A method used to check out whether there is unstable snapshot
func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && !IsEmptySnap(l.pendingSnapshot)
}

// A method to get snapshot
func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// A method used to check out whether the log is commited
// doubt, how to identify long lost append logs
func (l *RaftLog) checkLogCommitted(index uint64, target int) int {
	if l.committing[index].committed {
		return 2
	}
	if l.committing[index].counter > target/2 {
		l.committing[index].committed = true
		return 1
	}
	return 0
}

// A method used to build committed record
func (l *RaftLog) checkCommittedMapNil(index uint64) {
	if l.committing[index] == nil {
		l.committing[index] = &commitMsg{
			counter:   1,
			committed: false,
		}
	}
}
