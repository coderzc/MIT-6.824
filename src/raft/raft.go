package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const NoneID int = -1
const HeartbeatInterval = 300 * time.Millisecond
const MinElectionTimeout = 900 * time.Millisecond
const MaxElectionTimeout = 1800 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	leader    int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           NodeState
	heartbeatTicker *time.Ticker // leader send heartbeat
	electionTimer   *time.Timer  // random timeout start election

	currentTerm  int
	votedFor     int
	logs         []LogEntry
	lastLogIndex uint64

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[int]uint64
	matchIndex map[int]uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(
	server int, args *RequestVoteArgs,
	reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(
	server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ============================ RPC request handler ============================
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	requestTerm := args.Term
	if requestTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if requestTerm >= rf.currentTerm {
		rf.becomeFollower(requestTerm, args.LeaderId)
		if rf.state == Candidate {
			rf.votedFor = NoneID
		}
		reply.Term = requestTerm
		reply.Success = rf.appendLogs(args)
		return
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	requestTerm := args.Term

	if requestTerm < rf.currentTerm {
		rf.voteRefuse(reply, rf.currentTerm)
		return
	}

	if requestTerm > rf.currentTerm &&
		rf.isNewThanMe(args.LastLogIndex, args.LastLogTerm) {
		rf.voteGrant(reply, args.CandidateId, requestTerm)
		rf.becomeFollower(requestTerm, NoneID)
		return
	}

	switch rf.state {
	case Follower:
		if rf.votedFor == NoneID || rf.votedFor == args.CandidateId {
			if rf.isNewThanMe(args.LastLogIndex, args.LastLogTerm) {
				rf.voteGrant(reply, args.CandidateId, requestTerm)
			}
		}
		rf.voteRefuse(reply, rf.currentTerm)
	case Candidate: //已经给自己投过票
		rf.voteRefuse(reply, rf.currentTerm)
	case Leader:
		rf.voteRefuse(reply, rf.currentTerm)
	}
}

// ============================ RPC request handler ============================

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// ============================ heartsbeats recently.===========================
func (rf *Raft) ticker() {
	rf.resetElectionTimer()
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.tickElection()
		case <-rf.heartbeatTicker.C:
			rf.tickHeartbeat()
		default:
			DPrintf("a tick missed to fire. Node blocks too long!")
		}
	}
	rf.electionTimer.Stop()
	rf.heartbeatTicker.Stop()
}

func (rf *Raft) tickElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.state {
	case Follower:
		rf.becomeCandidate()
	case Candidate:
		rf.startElection()
	case Leader:
		// already election success
		log.Println("already election success")
	}
}

func (rf *Raft) tickHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		log.Printf("tick heartbeat on %v, but current is not leader, ignore", rf.me)
		return
	}
	for i, _ := range rf.peers {
		agrs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			LeaderCommit: 0,
		}
		reply := AppendEntriesReply{}
		if i != rf.me {
			go func(toId int) {
				ok := rf.sendAppendEntries(toId, &agrs, &reply)
				if ok {
					rf.handleAppendEntriesReply(reply)
				} else {
					log.Printf("sendReplicateLog failed, from:%v to:%v", rf.me, toId)
				}
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me

	log.Printf("startElection, term:%v, candidateId:%v", rf.currentTerm, rf.me)
	// 重置选举超时计时器
	rf.resetElectionTimer()
	lastLogTerm := -1
	if rf.lastLogIndex > 0 {
		lastLogTerm = rf.logs[rf.lastLogIndex-1].Term
	}
	reqVote := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	voteGrants := 0
	for index, _ := range rf.peers {
		go func(toId int) {
			reqVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(toId, &reqVote, &reqVoteReply)
			if ok {
				rf.mu.Lock()
				rf.handleRequestVoteReply(reqVoteReply, &voteGrants)
				rf.mu.Unlock()
			} else {
				log.Printf("sendRequestVote failed, from:%v to:%v", rf.me, toId)
			}
		}(index)
	}
}

// ============================ heartsbeats recently.===========================

// ============================ handle rpc Reply ===============================
func (rf *Raft) handleRequestVoteReply(reply RequestVoteReply, voteGrants *int) {
	replyTerm := reply.Term
	if replyTerm > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.becomeFollower(replyTerm, NoneID)
		return
	}

	if replyTerm < rf.currentTerm || !reply.VoteGranted {
		log.Printf("can't get votes, ignore it, RequestVoteReply: %v", reply)
		return
	}

	*voteGrants++
	if *voteGrants+1 > len(rf.peers)/2 {
		rf.becomeLeader()
	} else {
		rf.resetElectionTimer()
	}
}

func (rf *Raft) handleAppendEntriesReply(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, NoneID)
		// reset votedFor
		rf.votedFor = NoneID
	}

	if rf.state != Leader {
		log.Printf("recv append entries reply on %v, but current is not leader, ignore", rf.me)
		return
	}
}

// ============================ handle rpc Reply ===============================

// =================================== role transform ==========================
func (rf *Raft) becomeLeader() {
	if rf.state == Leader {
		return
	}
	log.Printf("voteGrants more than a half become to Leader, me:%v", rf.me)
	rf.electionTimer.Stop()
	rf.restHeartbeatTicker()
	rf.leader = rf.me
	rf.state = Leader
}

func (rf *Raft) becomeFollower(term int, leader int) {
	if rf.state == Follower {
		return
	}
	// stop heartbeat Ticker, because vote a new leader
	if rf.state == Leader {
		rf.heartbeatTicker.Stop()
	}
	// reset heartbeat timeout Timer
	rf.resetElectionTimer()
	// set leader
	rf.leader = leader
	rf.currentTerm = term
	rf.state = Follower
}

func (rf *Raft) becomeCandidate() {
	if rf.state == Candidate {
		return
	}
	rf.resetElectionTimer()
	rf.state = Candidate
}

// =================================== role transform ==========================

func (rf *Raft) voteGrant(reply *RequestVoteReply, voteFor int, latestTerm int) {
	reply.VoteGranted = true
	reply.Term = latestTerm
	rf.currentTerm = latestTerm
	rf.votedFor = voteFor
}

func (rf *Raft) voteRefuse(reply *RequestVoteReply, latestTerm int) {
	reply.VoteGranted = false
	reply.Term = latestTerm
}

func (rf *Raft) appendLogs(args *AppendEntriesArgs) bool {
	return true
}

func (rf *Raft) isNewThanMe(lastLogIndex uint64, lastLogTerm int) bool {
	myLastLogTerm := 0
	if rf.lastLogIndex > 0 {
		myLastLogTerm = rf.logs[rf.lastLogIndex-1].Term
	}

	if lastLogTerm > myLastLogTerm ||
		lastLogIndex >= rf.lastLogIndex {
		return true
	} else {
		return false
	}
}

func (rf *Raft) appliedLog() bool {
	if rf.commitIndex > rf.lastApplied {
		// 执行命令
		rf.lastApplied++
		return false
	} else {
		return true
	}
}

func (rf *Raft) resetElectionTimer() {
	var min = int64(MinElectionTimeout)
	var max = int64(MaxElectionTimeout)
	rf.electionTimer.Reset(time.Duration(rand.Int63n(max-min) + min))
}

func (rf *Raft) restHeartbeatTicker() {
	rf.heartbeatTicker.Reset(HeartbeatInterval)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.leader = NoneID
	rf.votedFor = NoneID
	rf.logs = make([]LogEntry, 100)
	rf.lastLogIndex = 0

	rf.heartbeatTicker = time.NewTicker(time.Second)
	rf.heartbeatTicker.Stop()
	rf.electionTimer = time.NewTimer(time.Second)
	rf.electionTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
