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
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const NoneID int = -1
const HeartbeatInterval = 300 * time.Millisecond
const MinElectionTimeout = 600 * time.Millisecond
const MaxElectionTimeout = 2000 * time.Millisecond
const MaxSize = 100
const MaxRetry = 1

type intSlice []int

func (p intSlice) Len() int           { return len(p) }
func (p intSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p intSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

	currentTerm int
	votedFor    int

	raftLog *raftLog

	prs map[int]*progress
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

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
	// Your code here (2B).

	index := rf.raftLog.lastIndex()
	term := rf.currentTerm
	isLeader := rf.leader == rf.me
	if !isLeader {
		return index, term, isLeader
	}
	//rf.appendEntries(LogEntry{Noop: false, Command: command, Term: term})
	return rf.raftLog.lastIndex(), rf.currentTerm, isLeader
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

// ============================ RPC request handler ============================
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		LogDebug("vote Refuse, args.Term < rf.currentTerm, currentTerm: %v, me:%v", rf.currentTerm, rf.me)
		rf.voteRefuse(reply, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, NoneID)
		if rf.isNewThanMe(args.LastLogIndex, args.LastLogTerm) {
			rf.voteGrant(reply, args.CandidateId, args.Term)
			return
		}
	}

	switch rf.state {
	case Follower:
		if rf.votedFor == NoneID || rf.votedFor == args.CandidateId {
			if rf.isNewThanMe(args.LastLogIndex, args.LastLogTerm) {
				rf.voteGrant(reply, args.CandidateId, args.Term)
				return
			}
		}
		LogDebug("vote Refuse, votedFor: %v, me:%v, Term:%v, args:%v, lastTerm:%v, lastIndex:%v", rf.votedFor, rf.me, rf.currentTerm, args, rf.raftLog.lastTerm(), rf.raftLog.lastIndex())
		rf.voteRefuse(reply, rf.currentTerm)
	case Candidate: //已经给自己投过票
		LogDebug("vote Refuse, candidate vote refuse")
		rf.voteRefuse(reply, rf.currentTerm)
	case Leader: // leader 不投票
		LogDebug("vote Refuse, leader vote refuse")
		rf.voteRefuse(reply, rf.currentTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.LastIndex = rf.raftLog.lastIndex()
		reply.Success = false
	} else {
		rf.becomeFollower(args.Term, args.LeaderId)
		reply.Term = args.Term

		if args.Entries == nil {
			//LogDebug("recv heart request, me:%v, Term:%v, leaderId:%v", rf.me, rf.currentTerm, rf.leader)
		} else {
			LogDebug("recv append request, me:%v, Term:%v, leaderId:%v, args:%v", rf.me, rf.currentTerm, rf.leader, args)
		}
		reply.Success = rf.copyEntries(args)
		reply.LastIndex = rf.raftLog.lastIndex()

		rf.raftLog.commitTo(args.LeaderCommit)
		rf.applyEntries()
	}
}

// ============================ RPC request handler ============================

// ============================ RPC request send function ======================
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
	for i := 0; i < MaxRetry; i++ {
		if rf.peers[server].Call("Raft.RequestVote", args, reply) {
			return true
		}
	}

	return false
}

func (rf *Raft) sendAppendEntries(
	server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	for i := 0; i < MaxRetry; i++ {
		if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
			return true
		}
	}
	return false
}

// ============================ RPC request send function ======================

// ================================ ticker =====================================
func (rf *Raft) tickHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		LogWarning("tick heartbeat at %v, but current is not leader, ignore", rf.me)
		return
	}
	rf.bcastHeartbeat()
}

// The ticker go routine starts a new election if this peer hasn't received
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
		LogInfo("already election success")
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me

	LogInfo("startElection, term:%v, candidateId:%v, leaderId:%v", rf.currentTerm, rf.me, rf.leader)
	// 重置选举超时计时器
	rf.resetElectionTimer()
	reqVote := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.raftLog.lastIndex(),
		LastLogTerm:  rf.raftLog.lastTerm(),
	}
	voteGrants := 0
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		reqVoteReply := RequestVoteReply{}
		go func(toId int) {
			if rf.sendRequestVote(toId, &reqVote, &reqVoteReply) {
				rf.handleRequestVoteReply(reqVoteReply, &voteGrants)
			} else {
				//LogWarning("sendRequestVote failed, from:%v to:%v", rf.me, toId)
			}
		}(index)
	}
}

// ================================ ticker =====================================

// ============================ handle rpc Reply ===============================
func (rf *Raft) handleRequestVoteReply(reply RequestVoteReply, voteGrants *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	replyTerm := reply.Term
	if replyTerm > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.becomeFollower(replyTerm, NoneID)
		return
	}

	if replyTerm < rf.currentTerm || !reply.VoteGranted {
		LogWarning("can't get votes, ignore it, RequestVoteReply: %v", reply)
		return
	}

	*voteGrants++
	if *voteGrants+1 >= rf.quorum() {
		rf.becomeLeader()
	} else {
		rf.resetElectionTimer()
	}
}

func (rf *Raft) handleHeartbeatReply(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, NoneID)
	}

	if rf.state != Leader {
		LogWarning("recv heartbeat reply at %v, but current is not leader, ignore", rf.me)
		return
	}
}

func (rf *Raft) handleAppendEntriesReply(id int, maybeCopyIndex int, reply AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term, NoneID)
	}

	pr := rf.prs[id]
	if !reply.Success {
		lastIndex := reply.LastIndex
		if pr.maybeDecrTo(maybeCopyIndex, lastIndex) {
			LogDebug("%v decreased progress of %v to [%v]", id, rf.me, pr)
			if pr.state == ProgressStateReplicate {
				pr.becomeProbe()
			}
		}
		return false
	} else {
		if pr.maybeUpdate(reply.LastIndex) {
			if pr.state == ProgressStateProbe {
				pr.becomeReplicate()
			}

			if rf.maybeCommit() {
				rf.bcastAppend()
			}
		}
		LogInfo("append entries success, toId:%v", id)
		return true
	}
}

// ============================ handle rpc Reply ===============================

// ============================== role transform ===============================
func (rf *Raft) becomeLeader() {
	if rf.state == Leader {
		return
	}
	LogInfo("a Leader be elected, leaderId:%v", rf.me)
	rf.electionTimer.Stop()
	rf.restHeartbeatTicker()
	rf.leader = rf.me

	// reset progress
	for id, pr := range rf.prs {
		pr.next = rf.raftLog.lastIndex() + 1
		pr.match = 0
		if id == rf.me {
			rf.prs[rf.me].match = rf.raftLog.lastIndex()
		}
	}

	rf.state = Leader

	// append Noop log
	rf.appendEntries(LogEntry{Noop: true})
}

func (rf *Raft) becomeFollower(term int, leader int) {
	//if rf.state == Follower && term == rf.currentTerm && leader == rf.leader{
	//	return
	//}

	// stop heartbeat Ticker, because vote a new leader
	if rf.state == Leader {
		rf.heartbeatTicker.Stop()
	}

	// set leader
	rf.leader = leader
	rf.votedFor = NoneID
	rf.currentTerm = term
	rf.state = Follower

	// reset heartbeat timeout Timer
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	if rf.state == Candidate {
		return
	}
	rf.resetElectionTimer()
	rf.state = Candidate
}

// ============================== role transform ===============================

func (rf *Raft) applyEntries() {
	for rf.raftLog.commitIndex > rf.raftLog.lastApplied {
		rf.raftLog.appliedTo(rf.raftLog.lastApplied + 1)
	}
}

func (rf *Raft) voteGrant(reply *RequestVoteReply, voteFor int, latestTerm int) {
	LogDebug("vote Grant, voteFor:%v, me:%v", voteFor, rf.me)
	rf.currentTerm = latestTerm
	rf.votedFor = voteFor
	reply.Term = latestTerm
	reply.VoteGranted = true
}

func (rf *Raft) voteRefuse(reply *RequestVoteReply, latestTerm int) {
	reply.Term = latestTerm
	reply.VoteGranted = false
}

func (rf *Raft) copyEntries(args *AppendEntriesArgs) bool {
	if args.Entries == nil {
		return false
	}

	if rf.raftLog.lastIndex() > args.PrevLogIndex {
		rf.raftLog.cutDown(args.PrevLogIndex)
	}

	if args.PrevLogTerm != rf.raftLog.lastTerm() {
		index := rf.raftLog.lastIndex()
		for ; rf.raftLog.lastTerm() != args.PrevLogTerm; index-- {
			rf.raftLog.cutDown(index)
		}
		return false
	} else {
		rf.raftLog.append(args.Entries...)
		return true
	}
}

func (rf *Raft) maybeCommit() bool {
	mis := make(intSlice, 0, len(rf.prs))
	for _, pr := range rf.prs {
		mis = append(mis, pr.match)
	}
	sort.Sort(sort.Reverse(mis))
	maxCommitIndex := mis[rf.quorum()-1]
	return rf.raftLog.maybeCommit(maxCommitIndex, rf.currentTerm)
}

func (rf *Raft) quorum() int {
	return len(rf.prs)/2 + 1
}

func (rf *Raft) appendEntries(entry ...LogEntry) {
	lastIndex := rf.raftLog.lastIndex()
	for _, logEntry := range entry {
		logEntry.Term = rf.currentTerm
		lastIndex++
		logEntry.Idx = lastIndex
		rf.raftLog.append(logEntry)
	}

	rf.bcastAppend()

	rf.maybeCommit()
}

func (rf *Raft) bcastAppend() {
	for id, _ := range rf.prs {
		if id == rf.me {
			continue
		}
		rf.sendAppend(id)
	}
}

func (rf *Raft) sendAppend(id int) {
	pr := rf.prs[id]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.leader,
		LeaderCommit: rf.raftLog.commitIndex,
		Entries:      rf.raftLog.entries(pr.next, MaxSize),
		PrevLogIndex: pr.next - 1,
		PrevLogTerm:  rf.raftLog.term(pr.next - 1),
	}
	reply := AppendEntriesReply{}
	if n := len(args.Entries); n != 0 {
		switch pr.state {
		case ProgressStateReplicate:
			pr.optimisticUpdate(args.Entries[n-1].Idx)
		case ProgressStateProbe:
			// thrift network flow
			args.Entries = args.Entries[:1]
		default:
			LogWarning("%x is sending append in unhandled state %v", rf.me, pr.state)
		}
	}
	go func(toId int) {
		if rf.sendAppendEntries(toId, &args, &reply) {
			if !rf.handleAppendEntriesReply(toId, args.PrevLogIndex+1, reply) {
				rf.mu.Lock()
				rf.sendAppend(toId)
				rf.mu.Unlock()
			}
		} else {
			LogWarning("sendAppendEntries failed, from:%v to:%v", rf.me, toId)
		}
	}(id)
}

func (rf *Raft) bcastHeartbeat() {
	for id, pr := range rf.prs {
		if id == rf.me {
			continue
		}

		commitIndex := min(pr.match, rf.raftLog.commitIndex)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.leader,
			LeaderCommit: commitIndex,
		}
		reply := AppendEntriesReply{}
		go func(toId int) {
			if rf.sendAppendEntries(toId, &args, &reply) {
				rf.handleHeartbeatReply(reply)
			} else {
				//LogWarning("sendHeartbeat failed, from:%v to:%v", rf.me, toId)
			}
		}(id)
	}
}

func (rf *Raft) isNewThanMe(lastLogIndex int, lastLogTerm int) bool {
	if lastLogTerm > rf.raftLog.lastTerm() ||
		lastLogIndex >= rf.raftLog.lastIndex() {
		return true
	} else {
		return false
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.raftLog = &raftLog{
		logs:         make([]LogEntry, 0, 100),
		lastLogIndex: 0,
		commitIndex:  0,
		lastApplied:  0,
		applyCh:      applyCh,
	}
	rf.prs = make(map[int]*progress)
	for id := range rf.peers {
		rf.prs[id] = &progress{next: 1}
	}

	rf.heartbeatTicker = time.NewTicker(time.Second)
	rf.heartbeatTicker.Stop()
	rf.electionTimer = time.NewTimer(time.Second)
	rf.electionTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run raft ticker
	go func() {
		rf.resetElectionTimer()
		for !rf.killed() {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			select {
			case <-rf.heartbeatTicker.C:
				rf.tickHeartbeat()
			case <-rf.electionTimer.C:
				rf.tickElection()
			default:
				//LogDebug("a tick missed to fire. Node blocks too long!")
			}
		}
		rf.electionTimer.Stop()
		rf.heartbeatTicker.Stop()
	}()

	return rf
}
