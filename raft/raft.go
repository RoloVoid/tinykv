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

// remember to regenerate vote through logs when doing 2B 2021.12.3
package raft

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
// noLimit means no limit when getting datas
const (
	None    uint64 = 0
	noLimit        = math.MaxUint64
)

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
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
	// Logger is used for raft log; learned from etcd
	Logger Logger
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

	// rolo: add one more validation check
	if c.Logger == nil {
		c.Logger = getLogger()
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// leader records the lastest match log and next log to send
	Match, Next uint64
	// rolo: doubt, whether should i add more related msgs
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// raft random time out
	random_ElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
	// a counter for check leader
	Majority *majority
	// logger from config
	logger Logger
}

// majority function
// a struct for leadercheck
// doubt
type majority struct {
	counterT int
	counterF int
}

//reset init doubt
func resetMajority() *majority {
	return new(majority)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// debug mode
	// temp, _ := c.Logger.(*DefaultLogger)
	// temp.EnableDebug()
	raftlog := newLogWithLogger(c.Storage, c.Logger)
	hs, cfs, err := c.Storage.InitialState()
	if err != nil {
		c.Logger.Panicf("create raft: something wrong when getting hs,cfs from storage:%s", err.Error())
	}
	// init votes,prs ---> temp, maybe change
	votes := make(map[uint64]bool)
	prs := make(map[uint64]*Progress)
	raft := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            votes,
		Prs:              prs,
		Majority:         resetMajority(), // for vote
		logger:           c.Logger,        // for log
	}
	lastindex := raft.RaftLog.LastIndex()
	// update prs
	if c.peers == nil {
		c.peers = cfs.Nodes
	}
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{Next: lastindex + 1, Match: 0}
	}
	// check if hardstate quarlified
	if !IsEmptyHardState(hs) {
		raft.loadState(hs)
	}
	// check if needed to reset applied
	if c.Applied > 0 {
		raftlog.setApplied(c.Applied)
	}
	// initialized as follower
	raft.becomeFollower(raft.Term, None)

	return raft
}

// loadstate check if hardstate is quarlified
func (r *Raft) loadState(hs pb.HardState) {
	if hs.Commit < r.RaftLog.committed || hs.Commit > r.RaftLog.LastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, hs.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
}

// check if there is a leader
func (r *Raft) hasLeader() bool {
	return r.Lead != None
}

// get softstate
func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// get hardstate
func (r *Raft) hardState() *pb.HardState {
	return &pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// a random tool, used for election randomized ---> inspired by etcd
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

// set random election time
func (r *Raft) resetRandomElectionTimeout() {
	r.random_ElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.random_ElectionTimeout
}

// inspired by etcd
func (r *Raft) reset() {
	r.resetRandomElectionTimeout()
	r.Majority = nil
	r.Majority = resetMajority()
	r.Majority.counterT++
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // vote for oneself
}

// send functions
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if !r.checkTargetPeer(to) {
		return false
	}

	pro := r.Prs[to]
	logterm, Terr := r.RaftLog.Term(pro.Next - 1)
	entries, Eerr := r.RaftLog.getEntries(pro.Next)
	// err when getting entries, then send snapshot
	if Terr != nil || Eerr != nil {
		return r.SendSnapshot(to)
	}

	var realentries []*pb.Entry
	if len(entries) == 0 || entries == nil {
		realentries = nil
	} else {
		for i := 0; i < len(entries); i++ {
			realentries = append(realentries, &entries[i])
		}
	}

	appendmsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.Prs[to].Next - 1,
		LogTerm: logterm,
		Entries: realentries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, appendmsg)
	return true
}

// doubt: send snapshot when needed
func (r *Raft) SendSnapshot(to uint64) bool {
	pro := r.Prs[to]
	msg := pb.Message{}
	msg.To = to
	msg.MsgType = pb.MessageType_MsgSnapshot
	snapshot, err := r.RaftLog.snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
			return false
		}
		panic(err) // TODO(bdarnell)
	}
	if IsEmptySnap(&snapshot) {
		panic("snapshot is empty; need non-empty snapshot")
	}
	msg.Snapshot = &snapshot
	sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
	r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
		r.id, r.RaftLog.FirstIndex(), r.RaftLog.committed, sindex, sterm, to, pro)
	r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pro)
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if !r.checkTargetPeer(to) {
		return
	}
	// doubt: leader transferee
	hbmsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, hbmsg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		}
		err := r.Step(msg)
		if err != nil {
			r.logger.Debug("handle election error")
		}
		if r.State == StateLeader && r.leadTransferee != None {
			r.leadTransferee = None
		}
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed = r.heartbeatElapsed + 1
	r.electionElapsed = r.electionElapsed + 1
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		}
		err := r.Step(msg)
		if err != nil {
			panic(err)
		}
		if r.State == StateLeader && r.leadTransferee != None {
			r.leadTransferee = None
		}
	}
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		msg := pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
		}
		if err := r.Step(msg); err != nil {
			panic(err)
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// term and related parameters updated
	r.State = StateFollower
	r.Lead = lead
	r.reset()
	r.Term = term
	// r.Vote = None	--->doubt
	// reset elapse
	// r.electionElapsed = 0
	// r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// related parameters update
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.reset()     // doubt,need to be completed
	r.Vote = r.id // vote for node itself
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// reset related parameter
	r.State = StateLeader
	r.Lead = None
	r.leadTransferee = None
	lastIndex := r.RaftLog.LastIndex()
	// reset prs
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0 // because if next is matched, the things behind it all matched
	}
	r.Prs[r.id].Match = r.Prs[r.id].Next
	r.Prs[r.id].Next += 1
	// noop entry, in order to make leader the newest
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.bcastAppend()
}

// bcast functions
func (r *Raft) bcastRequestVote() {
	li := r.RaftLog.LastIndex()
	lastlogTerm, _ := r.RaftLog.Term(li)
	for id := range r.Prs {
		// no msg for node itself
		if r.id == id {
			continue
		}
		// there is one check that won't vote for LastIndex() lesser than itself
		mrv := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   li,
			LogTerm: lastlogTerm,
		}
		r.msgs = append(r.msgs, mrv)
	}
}

// bcast:progress is still not used /doubt
func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if r.id == id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartBeat() {
	for id := range r.Prs {
		if r.id == id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

//response functions
func (r *Raft) responseToVote(m pb.Message) *pb.Message {
	// if m.Term < r.Term -----> reject
	if m.Term < r.Term {
		return &pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
	}

	// m.Term > r.Term
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = None
	}

	if m.Term > r.Term {
		r.Vote = None
		r.Term = m.Term
	}
	if r.Vote == m.From || r.Vote == None {
		li := r.RaftLog.LastIndex()
		lastlogTerm, _ := r.RaftLog.Term(li)
		if m.LogTerm > lastlogTerm ||
			(m.LogTerm == lastlogTerm && m.Index >= li) {
			r.Vote = m.From
			if r.Term < m.Term {
				r.Term = m.Term
			}
			return &pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
			}
		}
	}
	return &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
}

func (r *Raft) responseToHeartbeat(m pb.Message) *pb.Message {
	if m.Term < r.Term {
		return nil
	}
	r.electionElapsed = 0
	return &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      r.Lead,
		Term:    r.Term,
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

//case follower
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.campaign() {
			r.becomeCandidate()
			r.bcastRequestVote() // too clumsy /doubt
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		hbr := r.responseToHeartbeat(m)
		if hbr != nil {
			r.electionElapsed = 0
			r.msgs = append(r.msgs, *hbr)
		}
		return nil
	default:
		return nil
	}
	return nil
}

//case candidate
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.campaign() { //campaign and send msg_requestvote
			r.becomeCandidate()
			r.bcastRequestVote() // to clumsy;doubt
			return nil
		}
		return nil
	case pb.MessageType_MsgAppend: // doubt
		if m.Term < r.Term {
			return nil
		}
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		r.handleAppendEntries(m) //put this msg in its log and response
	case pb.MessageType_MsgRequestVote: //check and respond and update votes
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat: // require better design /doubt
		if m.Term < r.Term {
			return nil
		}
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		if m.Reject {
			r.Majority.counterF++
		} else if m.Reject == false {
			r.Majority.counterT++
		}
		if r.checkLeader() == 1 {
			r.becomeLeader()
			r.electionElapsed = 0
			r.heartbeatElapsed = 0
		} else if r.checkLeader() == 2 { //doubt
			r.becomeFollower(m.Term, m.From)
			r.electionElapsed = 0
		}
	default:
		return nil
	}
	return nil
}

//case leader
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: //leader won't recieve this
	case pb.MessageType_MsgBeat: // send heartbeat to all the followers
		r.bcastHeartBeat()
	case pb.MessageType_MsgPropose: // send msgAppend to all the followers
		if r.leadTransferee == None {
			r.handlePropose(m)
		}
	case pb.MessageType_MsgRequestVote: //doubt--->if leader recieve msr,reject
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppendResponse: //counter and check committed, update progress
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgSnapshot: //snapshot related
	case pb.MessageType_MsgHeartbeat:
		if m.Term <= r.Term { // ignore lower term msg
			return nil
		} else {
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		}
		return nil
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	default:
		return nil
	}
	return nil
}

// campaign method when election starts
func (r *Raft) campaign() bool {
	if len(r.Prs) < 2 {
		r.becomeCandidate()
		r.becomeLeader()
		return false
	}
	r.votes = nil
	newvotes := make(map[uint64]bool)
	r.votes = newvotes
	r.votes[r.id] = true
	return true
}

// handle functions
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reject := true
	if r.State != StateFollower && m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		reject = false
	}

	if m.Term >= r.Term {
		reject = false
		r.Term = m.Term
		r.Lead = m.From
	}

	// 1. one entry's logterm won't be smaller than statemachine's term
	if m.LogTerm > m.Term {
		reject = true
	}
	// 2. if one entry is stable but not equal, then reject
	if m.Index < r.RaftLog.stabled {
		term, _ := r.RaftLog.Term(m.Index)
		if m.LogTerm != term {
			reject = true
		}
	}
	// 3. sending msg
	rm := pb.Message{}
	if !reject {
		// get current raft lastindex
		for i, item := range m.Entries {
			lastIndex := r.RaftLog.LastIndex()
			var Term uint64
			item.Index = m.Index + uint64(i) + 1
			if item.Index <= lastIndex {
				Term, _ = r.RaftLog.Term(item.Index)
				if Term != item.Term {
					firstIndex := r.RaftLog.FirstIndex()
					if len(r.RaftLog.entries) != 0 && int64(m.Index-firstIndex+1) >= 0 {
						// 3. if not match,delete all the items behind it
						r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-firstIndex+1]
					}
					lastIndex = r.RaftLog.LastIndex()
					r.RaftLog.entries = append(r.RaftLog.entries, *item)
					r.RaftLog.stabled = m.Index
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, *item)
			}
		}
		// if not reject, update committed from leader
		r.RaftLog.committed = m.Commit
		rm = pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      r.Lead,
			Term:    m.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  reject,
		}
		r.msgs = append(r.msgs, rm)
		return
	}

	// if succeed,then response
	rm = pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}
	r.msgs = append(r.msgs, rm)
}

// handle append response
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgAppendResponse {
		return
	}
	// doubt: update progress based on reject and index
	if !m.Reject {
		for i := r.Prs[m.From].Next; i <= m.Index; i++ {
			if m.Term != 0 { // for noop
				r.checkCommittedMapNil(i)
				r.RaftLog.committing[i].counter++ // committed + 1
				// doubt
				var j int
				if j = r.checkLogCommitted(i); j == 1 {
					r.RaftLog.committed = i
					// send msgappend as committed to update follower's committed attribute
					r.sendAppend(m.From)
				}
			}
		}
		if m.Term != 0 {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
		}
	}
}

// handle requestvote
func (r *Raft) handleRequestVote(m pb.Message) {
	msg := r.responseToVote(m)
	r.msgs = append(r.msgs, *msg)
}

// handle propose for leader
func (r *Raft) handlePropose(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgPropose || m.From != r.id {
		return
	}
	// put entries to logs
	// doubt: currently there is only one session
	items := make([]pb.Entry, 0)
	for i := 0; i < len(m.Entries); i++ {
		// 1.check if propose is permitted
		// doubt
		m.Entries[i].Index = r.RaftLog.LastIndex() + uint64(len(r.RaftLog.entries))
		m.Entries[i].Term = r.hardState().Term
		// 2.put the entry into logs
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
		items = append(items, *m.Entries[i])
	}
	// doubt: 2.1.if lastindex == 0, updated storage ---->dirty because assert
	// storage, _ := r.RaftLog.storage.(*MemoryStorage)
	// storage.Append(items)
	// doubt: 2,2 if len(r.Prs) == 1, committed without append
	if len(r.Prs) < 2 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	// 3.bcastappend
	r.bcastAppend()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.MsgType != pb.MessageType_MsgHeartbeat {
		return
	}
	hbrm := r.responseToHeartbeat(m)
	r.msgs = append(r.msgs, *hbrm)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// check functions
// check if candidate becomes leader, very clumsy /doubt
func (r *Raft) checkLeader() int {
	target := len(r.Prs)
	if r.Majority.counterF > target/2 {
		return 2
	} else if r.Majority.counterT > target/2 {
		return 1
	}
	return 0
}

// check if log is committed
func (r *Raft) checkLogCommitted(index uint64) int {
	return r.RaftLog.checkLogCommitted(index, len(r.Prs))
}

// create one record
func (r *Raft) checkCommittedMapNil(index uint64) {
	r.RaftLog.checkCommittedMapNil(index)
}

// check if target peer exists
func (r *Raft) checkTargetPeer(to uint64) bool {
	_, ok := r.Prs[to]
	return ok
}
