package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type NodeState string

const (
	Leader    NodeState = "Leader"
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
)

type LogEntry struct {
	Noop    bool
	Command interface{}
	Term    int
	Idx     int
}

type Snapshot struct {
	Command interface{}
}

//type MessageType int32
//
//const (
//	MsgHup            MessageType = 0
//	MsgBeat           MessageType = 1
//	MsgProp           MessageType = 2
//	MsgApp            MessageType = 3
//	MsgAppResp        MessageType = 4
//	MsgVote           MessageType = 5
//	MsgVoteResp       MessageType = 6
//	MsgSnap           MessageType = 7
//	MsgHeartbeat      MessageType = 8
//	MsgHeartbeatResp  MessageType = 9
//	MsgUnreachable    MessageType = 10
//	MsgSnapStatus     MessageType = 11
//	MsgCheckQuorum    MessageType = 12
//	MsgTransferLeader MessageType = 13
//	MsgTimeoutNow     MessageType = 14
//	MsgReadIndex      MessageType = 15
//	MsgReadIndexResp  MessageType = 16
//	MsgPreVote        MessageType = 17
//	MsgPreVoteResp    MessageType = 18
//)

//============================== RPC ===================================//
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	LastIndex int
}

type RpcRaft interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply)

	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
}

//============================== RPC ===================================//
