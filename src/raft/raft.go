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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

// 节点三态
type Role int
const (
	Follower Role = iota + 1
	Candidate
	Leader
)

// 自定义Log Entries
type LogEntry struct {
	LogIndex   int
	LogTerm    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role  // 节点状态

	// Perisistent state on all servers
	currentTerm int  // 最新任期(初始为0, 单调递增)
	votedFor int  // candidateId
	voteCount int  // 获得选票数
	log []LogEntry   // log entries, 每个entry包含command 和对应的任期

	// Volatile state on all servers
	commitIndex int  // 最高被提交的entry的索引(初始为0, 单调递增)
	lastApplied int  // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leader
	nextIndex        []int
	matchIndex       []int

	// config
	heartbeatInterval int
	electionTimeoutMin int
	electionTimeoutRange int

	// channel
	voteResultChan chan bool
	heartBeatChan  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) GetLastLog() LogEntry {
	return rf.log[len(rf.log) - 1]
}

func (rf *Raft) GetPrevLog(server int) LogEntry {
	return rf.log[rf.nextIndex[server] - 1]
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int  // 候选人的任期号
	CandidateId int  // 请求选票的候选人的 Id
	LastLogIndex int  // 候选人的最后日志条目的索引值
	LastLogTerm int  // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool  // 候选人赢得了选票
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 忽略自身
	if args.CandidateId == rf.me {
		return
	}

	if (args.Term < rf.currentTerm) {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteCount = 0
		rf.votedFor = -1
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm < rf.GetLastLog().LogTerm {
			return
		}

		if args.LastLogTerm == rf.GetLastLog().LogTerm && args.LastLogIndex < rf.GetLastLog().LogIndex {
			return
		}

		reply.VoteGranted = true
		rf.role = Follower
		rf.votedFor = args.CandidateId
		rf.voteCount = 0
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {  // failure
		return ok
	}

	// 汇总选票，当Term小，蜕变为Follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if rf.role == Candidate {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.voteCount = 0
			rf.votedFor = -1
		} else if reply.VoteGranted  {
			rf.voteCount++
			if rf.voteCount > (len(rf.peers) / 2) {
				rf.BecomeLeader()
				rf.voteResultChan <- true   // 重置超时
			}
		}
	}
 	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// broadcast rpc msg
func (rf *Raft) BroadcastRequestVotes() {
	DPrintf("BroadcastRequestVotes|%d|make request vote.\n", rf.me)
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.GetLastLog().LogIndex
	args.LastLogTerm = rf.GetLastLog().LogTerm
	rf.mu.Unlock()
	for i := range rf.peers {
		// 也向自己发送投票
		go func(index int) {
			var reply RequestVoteReply
			rf.sendRequestVote(index, &args, &reply)
		}(i)
	}
}

func (rf *Raft) BroadcastAppendEntries() {
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.Entries = make([]LogEntry,0)
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	for i := range rf.peers {
		args.PreLogIndex = rf.GetPrevLog(i).LogIndex
		args.PreLogTerm = rf.GetPrevLog(i).LogTerm
		go func(index int) {
			var reply AppendEntriesReply
			rf.sendAppendEntries(index, &args, &reply)
		}(i)
	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.log = append(rf.log, LogEntry{LogIndex: 0, LogTerm: 0, Command: 0})

	rf.commitIndex = rf.log[0].LogIndex
	rf.lastApplied = rf.log[0].LogIndex

	// config
	rf.heartbeatInterval = 60
	rf.electionTimeoutMin = 150
	rf.electionTimeoutRange = 150

	// channel
	rf.heartBeatChan = make(chan bool, 100)
	rf.voteResultChan = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start
	go rf.ServerLoop()

	return rf
}

// 线程安全获取当前Rafe的Role
func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) ServerLoop() {
	DPrintf("ServerLoop|%d|start serverLoop. >>>\n", rf.me)
	for {
		switch rf.getRole() {
		case Follower:
			rf.FollowerLoop()
		case Candidate:
			rf.CandidateLoop()
		case Leader:
			rf.LeaderLoop()
		}
	}
}

func (rf *Raft) LeaderLoop() {
	// 转变为leader后应该广播心跳
	DPrintf("LeaderLoop|%d|broadcast AppendEntries RPC", rf.me)
	//go rf.broadcastAppendEntries()
	time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
}

func (rf *Raft) FollowerLoop() {
	select {
	case <- rf.heartBeatChan:
	case <- time.After(time.Duration(rf.electionTimeoutMin + rand.Intn(rf.electionTimeoutRange)) * time.Millisecond):
		rf.mu.Lock()
		rf.role = Candidate
		rf.mu.Unlock()
	}
}

func (rf *Raft) CandidateLoop() {
	// 发起选举
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()
	go rf.BroadcastRequestVotes()
	select {
	case win := <- rf.voteResultChan:
		if win {
			DPrintf("CandidateLoop|%d|belcome leader|voteCount:%d", rf.me, rf.voteCount)
		}
	case <- rf.heartBeatChan:
	case <- time.After(time.Duration(rf.electionTimeoutMin + rand.Intn(rf.electionTimeoutRange)) * time.Millisecond):
	}
}

func (rf *Raft) BecomeLeader() {
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		// TODO: review
		rf.nextIndex[i] = rf.GetLastLog().LogIndex
		rf.matchIndex[i] = 0
	}
}