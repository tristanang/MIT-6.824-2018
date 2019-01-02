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
    "sync"
    "labrpc"
    "time"
    "math/rand"
)

// import "bytes"
// import "labgob"

// Simple Functions
// Randomized timeouts between [400, 600)-ms
func electionTimeout() time.Duration { 
    return time.Duration(400 + rand.Intn(200)) * time.Millisecond
}

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState string

const (
    Leader ServerState = "Leader"
    Follower ServerState = "Follower"
    Candidate ServerState = "Candidate"
)

const HeartbeatInterval = 100 * time.Millisecond

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
    id          string
    state       ServerState

    // Persistent States
    currentTerm int
    votedFor    string
    //log

    // Volatile States
    commitIndex int
    lastApplied int

    // Leader Only States
    nextIndex   []int
    matchIndex  []int

    // Etc
    electionTimer *time.Timer
    timerChan      int bool
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

	var term int
	var isleader bool

    term = rf.currentTerm
    isleader = (rf.state == Leader)

	return term, isleader
}



// timer functions
func (rf *Raft) resetTimer() {
    select {
    case rf.timerChan <- true:
    default:
    }
}

func (rf *Raft) checkTimeout() {
    for {
        select {
        case <-rf.electionTimer.C:
            rf.mu.Lock()

            if rf.state == Follower {
                rf.becomeCandidate()
            } else if rf.state == Candidate {
                rf.startElection()
            }

            rf.mu.Unlock()

        case <-rf.timerChan:
            if !rf.electionTimer.Stop() {
                select {
                    case <- rf.electionTimer.C:
                    default:
                }
            }

            rf.electionTimer.Reset(electionTimeout())
        }
    }
}

// Functions related to electing a new leader
func (rf *Raft) becomeCandidate() {
    rf.state = Candidate
    rf.startElection()
}


func (rf *Raft) becomeFollower(term int) {
    rf.state = Follower
    rf.currentTerm = term
    rf.votedFor = ""
    rf.resetTimer()
}


func (rf *Raft) startElection() {

    rf.currentTerm++
    electionTerm = rf.currentTerm

    rf.votedFor = rf.id
    rf.resetTimer()

    voteChan := make(int bool, len(rf.peers) - 1)
    args := RequestVoteArgs {
            Term: rf.currentTerm,
            CandidateId: rf.id,
            LastLogIndex: 0,
            LastLogTerm: 0,
    }

    // requesting votes
    var wg sync.WaitGroup
    wait.Add(len(rf.peers) - 1)

    for i := range len(rf.peers) {
        if i != rf.me {
            reply := RequestVoteReply{}

            go func(i int, reply RequestVoteReply) {
                success := rf.sendRequestVote(i, &args, &reply)

                if success {
                    if reply.VoteGranted {
                        voteChan <- 1
                    } else {
                        voteChan <- 0
                        if reply.Term > rf.Term {
                            rf.becomeFollower(reply.Term)
                        }
                    }
                } else {
                    voteChan <- 0
                }

                wg.Done()
            }(i, reply)  
        }
    }

    wg.Wait()
    close(voteChan)

    // counting votes
    voteCount := 1
    for vote := range voteChan {
        voteCount += vote
    }

    if (rf.state != Candidate) || (rf.currentTerm != electionTerm)
    return
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId string
    LastLogIndex int
    LastLogTerm  int
}


//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}


func (rf *Raft) AppendEntries() {
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if args.Term > reply.Term {
        rf.becomeFollower(args.Term)
    }

    switch {
    case reply.Term > args.Term:
        reply.VoteGranted = false
    case (rf.votedFor == "" || rf.votedFor == args.CandidateId):
        reply.VoteGranted = true
        rf.votedFor = args.CandidateId
    default:
        DPrintf("default case")
        reply.VoteGranted = false
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
        peers:       peers,
        persister:   persister,
        me:          me,
        //
        id:          string(me),
        state:       Follower,
        currentTerm: 0,
        votedFor:   "",
        //
        commitIndex: 0,
        lastApplied: 0,
    }

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    // Election timeout
    rf.timerChan = make(chan bool)
    rf.electionTimer = time.NewTimer(electionTimeout())
    go rf.checkTimeout()

    return rf
}
