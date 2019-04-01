// Design Notes:
// 1) As much as possible, use mutex in helper functions.
// 2) Encapsulate indexing.

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
	// "bytes"
	"labgob"
	//
	crand "crypto/rand"
	"log"
	"math/big"
)

// seed random number generator
func init() {
	labgob.Register(LogEntry{})
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type ServerState string

const (
	Leader    ServerState = "Leader"
	Follower  ServerState = "Follower"
	Candidate ServerState = "Candidate"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type LogEntry struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	// General States
	state  ServerState
	leader int // identifies who the leader is

	// Persistent States
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile States
	commitIndex int
	lastApplied int

	// Leader Only States
	nextIndex  []int
	matchIndex []int

	// Timers
	electionTimer *time.Timer
	applyCh       chan ApplyMsg // apply to client

	// Shutdown
	shutdown chan bool
}

// Accessor Functions

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).

	term, state := rf.GetStateHelper()
	isleader = (state == Leader)

	return term, isleader
}

// Better GetState
func (rf *Raft) GetStateHelper() (int, ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state
}

// End accessor Functions

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

// Election Functions

const ElectionTimeout = 1000
const HeartbeatInterval = 100

// generate random time duration
func randTimeout() time.Duration {
	return time.Duration(ElectionTimeout+rand.Intn(ElectionTimeout)) * time.Millisecond
	// extra := time.Duration(rand.Int63()) % HeartbeatInterval 
	// return time.Duration((HeartbeatInterval + extra)) * time.Millisecond
}

func (rf *Raft) resetTimer() {
	// Always stop a electionTimer before reusing it. See https://golang.org/pkg/time/#Timer.Reset
	// We ignore the value return from Stop() because if Stop() return false, the value inside the channel has been drained out
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randTimeout())
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.Term, _ = rf.GetStateHelper()

	if reply.Term > args.Term {
		// DPrintf("%v refuses vote", rf.me)
		reply.VoteGranted = false
		return
	}

	// Since our term is smaller or equal to the other term, we compare.
	rf.compareTerm(args.Term)

	if rf.voteCheck(args.CandidateId) {
		reply.VoteGranted = true

		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
	} else {
		reply.VoteGranted = false
		// return
	}

	// I think this is the correct boolean...
	if reply.VoteGranted {
		rf.resetTimer()
	}

	return
}

func (rf *Raft) voteCheck(CandidateId int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.votedFor == -1 || rf.votedFor == CandidateId {
		return true
	} else {
		return false
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
	return ok
}

func (rf *Raft) compareTerm(otherTerm int) {

	myTerm, myState := rf.GetStateHelper()

	if otherTerm > myTerm {

		if myState != Follower {
			rf.becomeFollower(otherTerm)
		} else {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.currentTerm = otherTerm
			rf.votedFor = -1
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

	rf.mu.Unlock()

	rf.resetTimer()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v is becoming the leader.", rf.me)

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1 // initialized to leader's last log index + 1 (logs are one indexed)
			rf.matchIndex[i] = 0              // initialized to 0
		}
	}

	go rf.startHeartBeat() 
}

func (rf *Raft) startElection() {
	_, myState := rf.GetStateHelper()

	if myState == Leader {
		// rf.resetTimer()
		return
	}

	rf.mu.Lock()

	rf.leader = -1       // server believes there is no leader
	rf.state = Candidate // server is either a follower or a candidate. It needs to be a candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	DPrintf("Election: %v has term %v.", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	numPeers := len(rf.peers) // This is to avoid race condition
	me := rf.me               // This is to avoid race condition

	rf.resetTimer()

	rf.mu.Unlock()

	voteChan := make(chan RequestVoteReply, numPeers-1)

	for i := 0; i < numPeers; i++ {
		if i != me {
			go rf.askVote(i, args, voteChan)
		}
	}

	voteCount, threshold := 1, numPeers/2
	voted := 1


	for voteCount <= threshold && voted < numPeers {
		select {
		case <-rf.electionTimer.C: // election timeout
			return

		case reply := <-voteChan:

			if reply.VoteGranted {
				voteCount++
			} else {
				myTerm, _ := rf.GetStateHelper()

				if reply.Term > myTerm {
					rf.becomeFollower(reply.Term)
					break
				}
			}
		}
	}

	_, myState = rf.GetStateHelper()

	if voteCount <= threshold || myState != Candidate {
		return
	}

	// If we are here then we have won the election

	rf.becomeLeader()
}


func (rf *Raft) askVote(i int, args RequestVoteArgs, voteChan chan RequestVoteReply) {
	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(i, &args, &reply)

	if ok {
		voteChan <- reply
	} else {
		// go rf.askVote(i, args, voteChan)
	}
}

// End Election Functions

// Leader Functions

func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	numPeers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	// Initial Heartbeat (Optimization recommended by lab)
	for i := 0; i < numPeers; i++ {
		if i != me {
			go rf.sendHeartBeat(i)

		}
	}

	// Unsure about GC for tickers so a timer is used instead.
	timer := time.NewTimer(HeartbeatInterval * time.Millisecond)

	for {
		select {
		case <-rf.shutdown:
			return

		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

			for i := 0; i < numPeers; i++ {
				if i != me {
					go rf.sendHeartBeat(i)
				}
			}

			timer.Reset(HeartbeatInterval * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartBeat(i int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	args.Term, _ = rf.GetStateHelper()

	ok := rf.sendAppendEntries(i, &args, &reply)

	if ok {
		if reply.Success {
			return
		} else {
			rf.compareTerm(reply.Term)
		}
	} else {
		// rf.sendHeartBeat(i)
	}
}

// Append Entries functins
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term, _ = rf.GetStateHelper()

	if reply.Term > args.Term {
		reply.Success = false
		return
	}

	reply.Success = true
	rf.compareTerm(args.Term)
	rf.resetTimer()

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// End Leader Functions
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// rf.shutdown <- true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Follower
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		//
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		//
		commitIndex: 0,
		lastApplied: 0,
		//
		electionTimer: time.NewTimer(randTimeout()),
	}


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.startElection() // follower timeout, start a new election

			case <-rf.shutdown:
				return
			}
		}
	}()

	return rf
}

// Heartbeat Functions
