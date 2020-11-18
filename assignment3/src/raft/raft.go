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
	"bytes"
	"encoding/gob"
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

type logEntries struct {
	Command interface{}
	term    int
	index   int
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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []*logEntries

	// Additional variables
	electionTimeout int
	LeaderTimer     int

	votes int
	role  int
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct { //Implement first
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct { //Implement first
	// Your data here.
	VoteGranted bool
	CurrentTerm int
}

type AppendEntriesArgs struct {
	// payload TBD, log entry, piggyback commit, etc
	CurrentTerm       int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*logEntries
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//Handler for followers
	// When a candidate discovers that is term is out of date, it immediately reverts to follower state
	if rf.currentTerm <= args.CurrentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.role = FOLLOWER                        // Just in case it is a candidate
		rf.electionTimeout = 100 + rand.Int()%101 // reset election countdown
	}
	reply.Term = rf.currentTerm //Reply current Term
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//	Reject voting request if candidate is in a lower term
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		//	When a candidate or leader discovers that is term is out of date, it immediately reverts to follower state
		rf.role = FOLLOWER
		rf.electionTimeout = 100 + rand.Int()%101 // Starts Election timer
		rf.currentTerm = args.Term                // Update Term
		reply.VoteGranted = true
	}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool { //Must send to all
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
func Make(peers []*labrpc.ClientEnd, me int, // create a background goroutine that starts an election
	persister *Persister, applyCh chan ApplyMsg) *Raft { // (by sending out RequestVote RPCs) when it hasn't heard
	rf := &Raft{} // from another peer for a while
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	go rf.main()

	return rf
}

func (rf *Raft) main() {
	// Initial configuration for this server
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = FOLLOWER

	// Initialization of the server log
	rf.log = make([]*logEntries, 0)
	rf.log = append(rf.log, &logEntries{
		Command: nil,
		term:    0,
		index:   0,
	})

	// One of the first steps in every server is to start its election timeout
	rf.ResetElectionTimeout()

	// Start an infinite loop for this server and separate the handlers depending on the server's role
	for {
		if rf.role == FOLLOWER {
			rf.FollowerHandler()
		}
		if rf.role == CANDIDATE {
			rf.CandidateHandler()
		}
		if rf.role == LEADER {
			rf.LeaderHandler()
		}
	}
}

// ResetElectionTimeout resets the timeout to a random value between 150-300
func (rf *Raft) ResetElectionTimeout() {
	rf.electionTimeout = 150 + rand.Intn(150)
}

// FollowerHandler wraps the Raft server behavior for the case its role is FOLLOWER in the Main method
func (rf *Raft) FollowerHandler() {
	// The only continuos task for a FOLLOWER is reduce its own timeout
	time.Sleep(1 * time.Millisecond)
	rf.electionTimeout--

	// Once the timeout reaches 0, the server becomes a CANDIDATE
	if rf.electionTimeout < 0 {
		rf.role = CANDIDATE
	}
}

// CandidateHandler wraps the Raft server behavior for the case its role is CANDIDATE in the Main method
func (rf *Raft) CandidateHandler() {
	if rf.electionTimeout < 0 { //Create goroutine and send msgs
		rf.ResetElectionTimeout()
		rf.currentTerm++
		rf.votes = 1 // Voting for self

		for i := range rf.peers { // iterate through all peers
			if i == rf.me {
				// avoid sending auto RPC
				continue
			} else {
				// sending requests of votes to the rest
				// Creates goroutine to avoid blocking loop while waiting for reply
				go rf.nonBlockingSends("voteRequest", i)
			}
		}
	} else {
		if rf.votes > int(len(rf.peers)/2) {
			// Majority has been reached
			rf.role = LEADER
			rf.LeaderTimer = 0 // To send inmediatly a heatbeat
		} else {
			time.Sleep(1 * time.Millisecond)
			rf.electionTimeout--
		}
	}
}

// LeaderHandler wraps the Raft server behavior for the case its role is LEADER in the Main method
func (rf *Raft) LeaderHandler() {
	if rf.LeaderTimer < 0 {
		// Create goroutine and send heartbeat
		rf.LeaderTimer = 50 // milliseconds
		for i := range rf.peers {
			if i == rf.me {
				// avoid sending auto RPC
				continue
			} else {
				// sending requests of votes to the rest
				go rf.nonBlockingSends("appendEntry", i) // Creates goroutine to avoid blocking loop while waiting for reply
			}
		}
	} else {
		rf.LeaderTimer--
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) nonBlockingSends(msg string, index int) {
	if msg == "appendEntry" {
		reply := AppendEntriesReply{Term: -1, Success: false}
		rf.SendAppendEntries(index, AppendEntriesArgs{
			CurrentTerm:       rf.currentTerm,
			LeaderID:          rf.me,
			PrevLogIndex:      0,
			PrevLogTerm:       0,
			Entries:           nil,
			LeaderCommitIndex: 0,
		}, &reply)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.electionTimeout = 100 + rand.Int()%101 // reset election countdown
		}
	} else if msg == "voteRequest" {
		reply := RequestVoteReply{
			VoteGranted: false,
			CurrentTerm: -1,
		}

		rf.sendRequestVote(index, RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].index, rf.log[len(rf.log)-1].term}, &reply)

		if reply.VoteGranted == false {
			rf.currentTerm = reply.CurrentTerm // Update own Term
			rf.role = FOLLOWER                 // Converts in follower
		} else if reply.VoteGranted == true {
			rf.votes++
		}
	}
}
