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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// Constants for server roles
const (
	LEADER    = 1
	CANDIDATE = 2
	FOLLOWER  = 3
)

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

// LogEntry is the struct to hold a log entry in the raft servers
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm      int
	votedFor         int
	log              []*LogEntry
	electionTimeout  int
	heartbeatTimeout int
	votes            int
	role             int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm

	if rf.role == LEADER {
		isLeader = true
	}

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	VoteGranted bool
	CurrentTerm int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
}

// AppendEntries handles the entries append from leaders and updates its state accordingly.
// The request works as a heartbeat, the receiver resets its election timeout everytime it receives an AppendEntries call
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm <= args.Term {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.ResetElectionTimeout()
	}

	// Include the server term in the reply
	reply.Term = rf.currentTerm
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RequestVote handles the vote request from any other server and updates its state accordingly
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// When a raft server receives a vote request from another server, it should evaluate the term of the candidate and include
	// its vote in the reply
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true

		// If the current server receives a higher term than its own, the server becomes a FOLLOWER, updates its own term and resets
		// the election timeout
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.ResetElectionTimeout()
	}

	// Include the server term in the reply
	reply.CurrentTerm = rf.currentTerm
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start ...
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

	return index, term, isLeader
}

// Kill ...
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make ...
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

	// Your initialization code here.
	go rf.Main()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// Main is the method with the infinite loop to monitor each server behavior across the different roles
func (rf *Raft) Main() {
	// Initial configuration for this server
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	// Initialization of the server log
	rf.log = make([]*LogEntry, 0)
	rf.log = append(rf.log, &LogEntry{
		Command: nil,
		Term:    0,
	})

	// One of the first steps in every server is to start its election timeout
	rf.ResetElectionTimeout()

	// Start an infinite loop for this server and separate the handlers depending on the server's role
	for {
		if rf.role == LEADER {
			rf.LeaderHandler()
		} else if rf.role == FOLLOWER {
			rf.FollowerHandler()
		} else if rf.role == CANDIDATE {
			rf.CandidateHandler()
		}
	}
}

// ResetElectionTimeout resets the election timeout to a random value between 150-300
func (rf *Raft) ResetElectionTimeout() {
	rf.electionTimeout = 150 + rand.Intn(150)
}

// ResetHeartbeatTimeout resets the heartbet timeout to 30 milliseconds
// (It Complies with the timing requirement being minor than the election timeout)
func (rf *Raft) ResetHeartbeatTimeout() {
	rf.heartbeatTimeout = 30
}

// FollowerHandler wraps the Raft server behavior for the case its role is FOLLOWER in the Main method
func (rf *Raft) FollowerHandler() {
	// The only continuous task for a FOLLOWER is reduce its own timeout
	time.Sleep(1 * time.Millisecond)
	rf.electionTimeout--

	// Once the timeout reaches 0, the server becomes a CANDIDATE
	if rf.electionTimeout < 0 {
		rf.role = CANDIDATE
	}
}

// CandidateHandler wraps the Raft server behavior for the case its role is CANDIDATE in the Main method
func (rf *Raft) CandidateHandler() {
	// A Candidate is continuously checking its own election timeout
	if rf.electionTimeout < 0 {
		// If the election does not succeed during the election timeout, the server resets its timeout, increases its term and tries
		// again an election. A Candidate always starts the voting with its own vote.
		rf.ResetElectionTimeout()
		rf.currentTerm++
		rf.votes = 1

		// Iterate over all the servers to request their votes
		for i := range rf.peers {
			if rf.me != i {
				go rf.SendRequestVoteMessage(i)
			}
		}
	} else {
		// While the election timeout does not reach 0, the Candidate counts the positive votes it gets.
		// It requires the majority of votes to become the Leader
		requiredVotes := len(rf.peers) / 2

		if rf.votes > requiredVotes {
			rf.role = LEADER

			// Once it becomes the leader, make sure it sends a heartbeat immediately to prevent any other election
			rf.heartbeatTimeout = 0
		} else {
			// Reduce the election timeout every millisecond
			rf.electionTimeout--
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// LeaderHandler wraps the Raft server behavior for the case its role is LEADER in the Main method
func (rf *Raft) LeaderHandler() {
	// A leader is constantly checking its heartbeat timeout
	if rf.heartbeatTimeout < 0 {
		//  When the heartbeat timeout reaches 0, the leader resets its heartbeat timeout
		rf.ResetHeartbeatTimeout()

		// Also, the leader sends heartbeat messages to all the other servers
		for i := range rf.peers {
			// Do not send the heartbeat to itself
			if rf.me != i {
				go rf.SendAppendEntryMessage(i)
			}
		}
	} else {
		// Reduce the heartbeat timeout every millisecond
		rf.heartbeatTimeout--
		time.Sleep(1 * time.Millisecond)
	}
}

// SendAppendEntryMessage is a bridge method to build the AppendEntriesReply object, invoke the SendAppendEntries method in
// the given server and reset the server state in case the replied term is larger
func (rf *Raft) SendAppendEntryMessage(serverIndex int) {
	// Build a dummy AppendEntriesReply object to store the response
	reply := AppendEntriesReply{-1}

	// Build an AppendEntriesArgs object and call the SendAppendEntries method in the given server
	rf.SendAppendEntries(serverIndex, AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, 0}, &reply)

	// Evaluate the response. If the received term is larger than the current server, means a new leader was elected and
	// the current server has to reset its state
	if reply.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = reply.Term
		rf.ResetElectionTimeout()
	}
}

// SendRequestVoteMessage is a bridge method to build the RequestVoteReply object, invoke the SendRequestVote method in
// the given server and align the server state in case the vote is negative
func (rf *Raft) SendRequestVoteMessage(serverIndex int) {
	// Build dummy RequestVoteReply object to store the response
	reply := RequestVoteReply{
		VoteGranted: false,
		CurrentTerm: -1,
	}

	// Build an RequestVoteArgs object and call the sendRequestVote method in the given server
	rf.SendRequestVote(serverIndex, RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.log[len(rf.log)-1].Index,
		rf.log[len(rf.log)-1].Term}, &reply)

	// Evaluate the response to either increase the votes count or to align the server state with the response
	if reply.VoteGranted {
		rf.votes++
	} else {
		rf.currentTerm = reply.CurrentTerm
		rf.role = FOLLOWER
	}
}
