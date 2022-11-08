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
	Term    int
}

type RevertToFol struct {
	Term  int
	Vote  int
	Reset bool
}

// AppendEntriesArgs
// ===========
//
// Args for AppendEntries RPC are as per the paper specifics
// term is the leader's term
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entires      []LogEntry
	LeaderCommit int
}

//AppendEntriesReply
//
// Replies with current term for leader to update itself and bool success if the follower contained
// entry matching prevLogIndex and prevLogTerm

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	Mux   sync.Mutex       // Lock to protect shared access to this peer's state
	Peers []*rpc.ClientEnd // RPC end points of all peers
	Me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	Logger *log.Logger // We provide you with a separate logger per peer.

	Timeout        *time.Timer
	CurrentTerm    int
	CurrentRole    int
	Logs           []LogEntry
	RevertToFol    chan RevertToFol
	RevertComplete chan bool

	VotedFor  int
	ExitQueue chan bool
	ExitCheck chan bool
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
	rf.Mux.Lock()
	defer rf.Mux.Unlock()

	//Grab term from raft
	term = rf.CurrentTerm
	me = rf.Me
	//Check to see if the raft is the leader, 1 = leader, 2 = candidate, 3 = follower
	if rf.CurrentRole == 1 {

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
	Term         int //candidates term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidates last log entry
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
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate recieved vote
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.Mux.Lock()
	defer rf.Mux.Unlock()

	//Case 1
	//Check to see if term is lower (out of date)
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm //Bring the term up to date
		reply.VoteGranted = false   //reject vote since the term is stale
		return
	}
	//current log index
	lastLogIndex := len(rf.Logs) - 1
	//current log term
	lastLogTerm := rf.Logs[lastLogIndex].Term

	//Case 2
	//See if the lastLogterm is below current, or, equal and the the index is lesser. If so, update the term & reject the vote
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {

		reply.Term = args.Term
		reply.VoteGranted = false

	} else if rf.CurrentTerm < args.Term {
		//Case 3
		//Term is not stale, ok to grant vote
		reply.Term = args.Term
		reply.VoteGranted = true

	} else {
		//Case 4
		//Term is stale, not valid, bring up to date & reject vote
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false

	}

	//Discovers term is out of date, revert to follower here

	if rf.CurrentTerm < args.Term {

		//create follower reversion based on current state of node
		var revert RevertToFol = RevertToFol{args.Term, args.CandidateId, reply.VoteGranted}
		rf.RevertToFol <- revert //queue reversion
		<-rf.RevertComplete      //Push complete for confirmation
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
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) elecTimeout() int {

	randOffset := rand.New(rand.NewSource(time.Now().UnixMilli()))
	return eTimeout + randOffset.Intn(200)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("[sendAppendEntries] to server:%d request:%v", server, args)
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Leader() {

	go func() {
		rf.Mux.Lock()
		defer rf.Mux.Unlock()
		//Only valid conversion if peer is a candidate
		if rf.CurrentRole != 2 {

			return
		}
		rf.CurrentRole = 1

	}()

	for true {

		select {

		case norm := <-rf.RevertToFol:
			//revert to a follower
			go rf.makeFollower(norm)
			return
		case <-rf.ExitQueue:
			return

		default:

			//Make sure you're a leader, so you can send out the heartbeat
			if rf.CurrentRole == 1 {
				totalPeers := len(rf.Peers)

				for i := 0; i < totalPeers; i++ {
					if i != rf.Me {

						go func(peerNum int) {

							var reply AppendEntriesReply

							var args AppendEntriesArgs

							args.Term = rf.CurrentTerm
							args.LeaderId = rf.Me

							rf.AppendEntries(&args, &reply)

						}(i)

					}

				}
				time.Sleep(hbInterval * time.Millisecond)
			}
		}

	}

}

func (rf *Raft) Candidate() {

	fmt.Println("Candidate started")
	rf.CurrentRole = 2

	for true {

		//Start an election the moment I'm a candidate
		voteOutcome := make(chan bool, len(rf.Peers))
		go rf.electionProcess(voteOutcome)
		//Reset the timeout
		rf.Timeout.Reset(time.Duration(rf.elecTimeout()) * time.Millisecond)

		select {

		case norm := <-rf.RevertToFol:
			//Got a hb from a new leader, back to being a boring follower
			go rf.makeFollower(norm)
			return
		case <-rf.ExitQueue:
			return
		case winner := <-voteOutcome:
			//I won the election! Time to make some changes around here... or just continue the regime
			if winner == true {

				go rf.Leader()
				return
			}
		case <-rf.Timeout.C:
			//Repeat the process until there is a new leader, essentially a 'continue'
		}

	}

}

func (rf *Raft) makeFollower(fol RevertToFol) {

	rf.CurrentRole = 3
	if rf.CurrentTerm < fol.Term {
		rf.CurrentTerm = fol.Term
	}
	if fol.Vote != -1 {
		rf.VotedFor = fol.Vote

	}
	rf.RevertComplete <- true
	if fol.Reset == true {
		rf.Timeout.Reset(time.Duration(rf.elecTimeout()) * time.Millisecond)
	}

	rf.Follower()

}

func (rf *Raft) Follower() {

	rf.CurrentRole = 3

	//Run this loop while in follower mode
	for true {

		select {

		case norm := <-rf.RevertToFol:
			//Normal default case, remain a follower listening for heartbeats until something interesting happens
			if norm.Term > rf.CurrentTerm {

				go rf.makeFollower(norm)
				return

			}
			rf.RevertComplete <- true
			if norm.Reset == true {

				rf.Timeout.Reset(time.Duration(rf.elecTimeout()) * time.Millisecond)
			}
		case <-rf.Timeout.C:
			//No heartbeat! Time for something interesting, let's be a candidate!
			go rf.Candidate()
			return

		case <-rf.ExitQueue:
			return
		}

	}

}

func (rf *Raft) electionProcess(winner chan bool) {

	//On conversion to candidate, start election:
	//• Increment currentTerm
	//• Vote for self
	//• Reset election timer
	//• Send RequestVote RPCs to all other servers
	//• If votes received from majority of servers: become leader
	//• If AppendEntries RPC received from new leader: convert to
	//follower
	//TODO If election timeout elapses: start new election

	maxVotes := len(rf.Peers)
	voteList := make([]bool, maxVotes)

	rf.Mux.Lock()

	//Make sure I'm a candidate
	if rf.CurrentRole != 2 {
		rf.Mux.Unlock()
		return
	}

	rf.CurrentTerm += 1 //increment the term
	rf.VotedFor = rf.Me //vote for self

	voteList[rf.Me] = true //register my vote on the list

	//Init request to broadcast to other peers
	var voteBroad RequestVoteArgs
	voteBroad.Term = rf.CurrentTerm
	voteBroad.CandidateId = rf.Me
	//voteBroad.lastLogIndex = len(rf.logs) - 1
	//voteBroad.lastLogTerm = rf.logs[voteBroad.lastLogIndex].term
	rf.Mux.Unlock()

	fmt.Println("Good to here")

	for i := 0; i < maxVotes; i++ {
		if i != rf.Me {

			go func(voteList []bool, peer_num int) {

				fmt.Println("Ok Here")

				//Send out vote requests to see reply
				var reply RequestVoteReply
				confirm := rf.sendRequestVote(peer_num, &voteBroad, &reply)

				fmt.Println("OK")

				voteList[peer_num] = confirm && reply.VoteGranted

				//Check to see if I have majority
				count := 0
				for j := 0; j < maxVotes; j++ {
					if voteList[j] {
						count++

					}

				}
				//If vote tally is atleast majority, we have a winner
				if count >= (maxVotes/2)+1 {

					winner <- true
					fmt.Println("Winner made")
				}

			}(voteList, i)

		}

	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//prevent race cases
	rf.Mux.Lock()
	defer rf.Mux.Unlock()

	//Case 1: term < current term, bad, reply false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm //update the term
		reply.Success = false       //not successful

	} else {

		lgLenLess := len(rf.Logs) < args.PrevLogIndex+1
		lgNoMatch := !lgLenLess && args.PrevLogIndex > 0 && (rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm)
		//Case 2 : Reply false if log doesn't contain entry at prevLogIndex or if it does, but doesn't match
		if lgLenLess || lgNoMatch {
			reply.Term = args.Term
			reply.Success = false
		} else {

			//Case 3: If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			if len(rf.Logs)-1 != args.PrevLogIndex {
				rf.Logs = rf.Logs[:args.PrevLogIndex+1]
			}
			//Case 4:  Append any new entries not already in the log
			rf.Logs = append(rf.Logs, args.Entires...)

			//TODO Add case 5

			reply.Term = args.Term
			reply.Success = true
		}

	}

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
	rf.Peers = peers
	rf.Me = me

	//Start the peer as a follower
	rf.RevertToFol = make(chan RevertToFol)
	rf.RevertComplete = make(chan bool)
	rf.ExitQueue = make(chan bool)
	rf.ExitCheck = make(chan bool)

	rf.CurrentTerm = 0                                                             //starting count at 0
	rf.VotedFor = -1                                                               //Haven't voted for anyone yet
	rf.Timeout = time.NewTimer(time.Duration(rf.elecTimeout()) * time.Millisecond) //setup election timeout

	go rf.Follower() //start life as a follower

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.Logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.Logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.Logger.Println("logger initialized")
	} else {
		rf.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)

	return rf
}
