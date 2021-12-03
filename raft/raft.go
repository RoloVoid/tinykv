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
	"math/rand"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

// a random tool from etcd
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
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

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
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
	raftlog := newLog(c.Storage)
	votes := make(map[uint64]bool)
	//init prs
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[id] = new(Progress)
	}
	raft := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            votes,
		Prs:              prs,
		Majority:         resetMajority(),
	}

	return raft
}

// set random election time
func (r *Raft) resetRandomElectionTimeout() {
	r.random_ElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
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
	r.votes = nil
	// vote for oneself
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// get softstate and hardstate
func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() *pb.HardState {
	return &pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// send functions
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// if r.id == to {
	// 	return false
	// } // rebuild
	// doubt whether logs are added like this
	appendmsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, appendmsg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// if r.id == to {
	// 	return
	// }
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
	// doubt
	if r.pastElectionTimeout() {
		// if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		}
		err := r.Step(msg)
		//doubt
		if err != nil {
			panic(err)
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

// check commit size, inspired by etcd
func (r *Raft) reduceUncommitedSize(ents []pb.Entry) {
	if r.RaftLog.unstableEntries() == nil || len(r.RaftLog.unstableEntries()) <= 0 {
		return
	}
}

// handle ready and update, inspired by etcd
func (r *Raft) advance(rd Ready) {}

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
	r.reset()     //doubt,need to be completed
	r.Vote = None // vote for itself,but do not record--->doubt
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// reset related parameter
	r.State = StateLeader
	r.Lead = None // asign leader to none when node is leader
	// r.Term++
	// reset elapse
	// r.electionElapsed = 0
	// r.heartbeatElapsed = 0
}

// bcast functions
func (r *Raft) bcastRequestVote() {
	for id := range r.Prs {
		// no msg for node itself--doubt
		if r.id == id {
			continue
		}
		mrv := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
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
	check := false
	// maybe not here /doubt
	if m.Term < r.Term || (r.Vote != None && r.Vote != m.From) || (m.Term == r.Term && r.State != StateFollower) {
		check = true
	}
	if !check {
		r.Vote = m.From
	}
	return &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  check,
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
		return nil
	case pb.MessageType_MsgAppend: //doubt
		//put this msg in its log and response
		if m.Term <= r.Term {
			return nil
		}
		r.becomeFollower(m.Term, m.From)
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		// figure out a better design /doubt
		mrvr := r.responseToVote(m)
		if !mrvr.Reject { // if not reject, become follower
			r.becomeFollower(m.Term, None)
		}
		r.msgs = append(r.msgs, *mrvr)
		return nil
	case pb.MessageType_MsgHeartbeat: //doubt
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
	case pb.MessageType_MsgHup: // figure out a better design /doubt
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
		r.handleAppendEntries(m)
		//put this msg in its log and response
	case pb.MessageType_MsgRequestVote: //check and respond and update votes
		mrvr := r.responseToVote(m) //not a good design /doubt
		if !mrvr.Reject {
			r.becomeFollower(m.Term, None) // if not reject, become follower
		}
		r.msgs = append(r.msgs, *mrvr)
	case pb.MessageType_MsgHeartbeat:
		// require better design /doubt
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
			r.bcastAppend()
			//doubt
		} else if r.checkLeader() == 2 {
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
		// r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote: //doubt--->if leader recieve msr,reject

		mrvr := r.responseToVote(m)
		if !mrvr.Reject {
			r.becomeFollower(m.Term, None)
		}
		r.msgs = append(r.msgs, *mrvr)
	case pb.MessageType_MsgAppendResponse: //counter and check committed
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
		r.becomeFollower(m.Term, m.From)
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
	if !(m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgPropose) {
		return
	}
	if m.Term >= r.Term {
		r.Term = m.Term
	} else {
		return
	}
	// add entry to its own log
	for _, item := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *item)
	}
	// if succeed,then response
	rm := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      r.Lead,
		Reject:  false,
	}
	r.msgs = append(r.msgs, rm)
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
