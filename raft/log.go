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

type commitMsg struct {
	counter   int
	committed bool
	applied   bool
}

func errHandler(err error) {
	if err != nil {
		panic(err)
		//log.Errorf(err.Error())
	}
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastindex, err := storage.LastIndex()
	errHandler(err)
	// fi, err := storage.FirstIndex()
	// errHandler(err)
	// doubt ---> assert is dirty
	// doubt ---> storage is not
	entries := make([]pb.Entry, 0)
	// initialize, all the pointers are at the startline
	NewRaftLog := &RaftLog{
		storage:    storage,
		committed:  lastindex,
		applied:    lastindex,
		stabled:    lastindex,
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
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (l *RaftLog) maybeFirstIndex() (uint64, bool) {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (l *RaftLog) maybeLastIndex() (uint64, bool) {
	// if there are unstable entries, stabled + unsstabled - 1 = lastindex
	// len(l.entries)=0, means there might be a compact action,which means a snapshot
	if len := len(l.entries); len != 0 {
		return l.stabled + uint64(len) - 1, true
	}
	// if there is no stable entries, then get last index from snapshot
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

// check if term is available
func (l *RaftLog) maybeTerm(i uint64) (uint64, bool) {
	// ---> if request index < current stabled, get it from snapshot
	if i < l.stabled {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := l.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return l.entries[i-l.stabled].Term, true
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries
// rolo:it is used for build ready
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	last := max(l.applied+1, l.FirstIndex())
	if l.committed+1 > last {
		ents, err := l.pieces(last, l.committed+1)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// LastIndex returns the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// get last index from unstable | snapshot
	if i, ok := l.maybeLastIndex(); ok {
		return i
	}
	// if not , get it from storage
	li, err := l.storage.LastIndex()
	if err != nil {
		l.logger.Panicf("something went wrong when getting first index from storage: %s", err.Error())
		panic(err)
	}
	return li
}

// FirstIndex returns the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	// get first index from unstable | snapshot
	if i, ok := l.maybeFirstIndex(); ok {
		return i
	}
	// if not, get it from storage
	fi, err := l.storage.FirstIndex()
	if err != nil {
		l.logger.Panicf("something went wrong when getting first index from storage: %s", err.Error())
		panic(err)
	}
	return fi
}

// Term returns the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.FirstIndex()-1 || i > l.LastIndex() {
		return 0, nil
	}
	// get term from unstable | snapshot
	if t, ok := l.maybeTerm(i); ok {
		return t, nil
	}
	// if not, get it from storage
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	} else {
		l.logger.Panicf("something went wrong in log.Term when trying to get term from storage, unexpected errors")
		panic(err)
	}
}

// getEntries gets from lo to lastindex
// rolo: currently ,there is no need to get entries;
// but if there is a new node for a group that works for a long time, it will be slower
func (l *RaftLog) getEntries(lo uint64) ([]pb.Entry, error) {
	if lo > l.LastIndex() {
		return nil, nil
	}
	return l.pieces(lo, l.LastIndex()+1)
}

// piecesUnstabled returns a target piece from unstable entries [lo,hi]
func (l *RaftLog) piecesUnstabled(lo, hi uint64) []pb.Entry {
	l.mustCheckOutOfBoundsUnstable(lo, hi)
	return l.entries[lo-l.stabled : hi-l.stabled]
}

// pieces returns the target entries with given range [lo,hi)
func (l *RaftLog) pieces(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	// if lo comes in the storage, then get it from storage
	if lo < l.stabled {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.stabled))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.stabled)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	// means there are still some unstable entries need to be committed
	if hi > l.stabled {
		unstable := l.piecesUnstabled(max(lo, l.stabled), hi)
		if len(ents) > 0 {
			// then put stable and unstable entries together to apply
			all := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(all, ents)
			copy(all[n:], unstable)
			ents = all
		} else {
			ents = unstable
		}
	}
	return ents, nil
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
	if l.committing[index].applied {
		// delete(l.committing, index) // ------> delete applied record
		return 3
	}
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
			applied:   false,
		}
	}
}

// boundary check --> stable and unstable
// l.stabled <= lo <= hi <= u.stabled+len(u.entries)
func (l *RaftLog) mustCheckOutOfBoundsUnstable(lo, hi uint64) {
	if lo > hi {
		l.logger.Panicf("invalid unstable.piece %d > %d", lo, hi)
	}
	upper := l.stabled + uint64(len(l.entries))
	if lo < l.stabled || hi > upper {
		l.logger.Panicf("unstable.piece[%d,%d) out of bound [%d,%d]", lo, hi, l.stabled, upper)
	}
}

// l.FirstIndex <= lo <= hi <= l.FirstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.FirstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}
