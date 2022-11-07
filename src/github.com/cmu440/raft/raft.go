//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const hbInterval = 100 //Sends heartbeat 10 times per second, no more than that per project specifics

// TODO Need to find proper timeout interval, this is a placeholder (Need to add randomization as per paper)
const eTimeout = 400 //Election timeout

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// LogEntry
// ====
//
// Log entry, per paper outline
type LogEntry struct {
	Command interface{}
	term    int
}

type revertToFol struct {
	term  int
	vote  int
	reset bool
}

// AppendEntriesArgs
// ===========
//
// Args for AppendEntries RPC are as per the paper specifics
// term is the leader's term
type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entires      []LogEntry
	leaderCommit int
}

//AppendEntriesReply
//
// Replies with current term for leader to update itself and bool success if the follower contained
// entry matching prevLogIndex and prevLogTerm

type AppendEntriesReply struct {
	term    int
	success bool
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	cTerm          int
	cRole          int
	logs           []LogEntry
	revertToFol    chan revertToFol
	revertComplete chan bool

	votedFor  int
	exitQueue chan bool
	exitCheck chan bool
	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain

}

// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool

	//Lock to prevent race cases
	rf.mux.Lock()
	defer rf.mux.Unlock()

	//Grab term from raft
	term = rf.cTerm

	//Check to see if the raft is the leader, 1 = leader, 2 = candidate, 3 = follower
	if rf.cRole == 1 {

		isleader = true
	} else {

		isleader = false
	}

	return me, term, isleader
}

// RequestVoteArgs
// ===============
//
// # Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteArgs struct {
	//Candidate's term, candidates requesting vote, index of candidates's last log entry, term of candidate's last log entry
	term         int //candidates term
	candidateId  int //candidate requesting vote
	lastLogIndex int //index of candidate's last log entry
	lastLogTerm  int //term of candidates last log entry
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteReply struct {
	//Needs yes or no on vote's outcome, along with the current term to check against per the paper specifics
	term        int  //currentTerm, for candidate to update itself
	voteGranted bool //true means candidate recieved vote
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mux.Lock()
	defer rf.mux.Unlock()

	//Case 1
	//Check to see if term is lower (out of date)
	if args.term < rf.cTerm {
		reply.term = rf.cTerm     //Bring the term up to date
		reply.voteGranted = false //reject vote since the term is stale
		return
	}

	//current log index
	lastLogIndex := len(rf.logs) - 1
	//current log term
	lastLogTerm := rf.logs[lastLogIndex].term

	//Case 2
	//See if the lastLogterm is below current, or, equal and the the index is lesser. If so, update the term & reject the vote
	if args.lastLogTerm < lastLogTerm || (args.lastLogTerm == lastLogTerm && args.lastLogIndex < lastLogIndex) {

		reply.term = args.term
		reply.voteGranted = false

	} else if rf.cTerm < args.term {
		//Case 3
		//Term is not stale, ok to grant vote
		reply.term = args.term
		reply.voteGranted = true

	} else {
		//Case 4
		//Term is relatively stale, not valid, bring up to date & reject vote
		reply.term = rf.cTerm
		reply.voteGranted = false

	}

	//Discovers term is out of date, revert to follower here

	if rf.cTerm < args.term {

		//create follower reversion based on current state of node
		var revert revertToFol = revertToFol{args.term, args.candidateId, reply.voteGranted}
		rf.revertToFol <- revert //queue reversion
		<-rf.revertComplete      //Push complete for confirmation
	}

}

// sendRequestVote
// ===============
//
// # Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// # Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// # Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// # If this server is not the leader, return false
//
// # Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// # The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// Your code here, if desired
}

// NewPeer
// ====
//
// # The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	//Start the peer as a follower
	rf.revertToFol = make(chan revertToFol)
	rf.revertComplete = make(chan bool)

	rf.cTerm = 0

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)

	return rf
}

func (rf *Raft) elecTimeout() int {

	randOffset := rand.New(rand.NewSource(time.Now().UnixMilli()))
	return eTimeout + randOffset.Intn(200)

}

func (rf *Raft) makeLeader() {}

func (rf *Raft) makeCandidate() {}

func (rf *Raft) makeFollower() {}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//prevent race cases
	rf.mux.Lock()
	defer rf.mux.Unlock()

	//Case 1: term < current term, bad, reply false
	if args.term < rf.cTerm {
		reply.term = rf.cTerm //update the term
		reply.success = false //not successful

	} else {

		lgLenLess := len(rf.logs) < args.prevLogIndex+1
		lgNoMatch := !lgLenLess && args.prevLogIndex > 0 && (rf.logs[args.prevLogIndex].term != args.prevLogTerm)
		//Case 2 : Reply false if log doesn't contain entry at prevLogIndex or if it does, but doesn't match
		if lgLenLess || lgNoMatch {
			reply.term = args.term
			reply.success = false
		} else {

			//Case 3: If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			if len(rf.logs)-1 != args.prevLogIndex {
				rf.logs = rf.logs[:args.prevLogIndex+1]
			}
			//Case 4:  Append any new entries not already in the log
			rf.logs = append(rf.logs, args.entires...)

			//TODO Add case 5

		}

	}

}
