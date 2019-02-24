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

// import "bytes"
// import "labgob"

// Simple Functions
// Randomized timeouts between [400, 600)-ms
func electionTimeout() time.Duration {
	return time.Duration(600+rand.Intn(200)) * time.Millisecond
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
type LogEntry struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState string

const (
	Leader    ServerState = "Leader"
	Follower  ServerState = "Follower"
	Candidate ServerState = "Candidate"
)

const HeartbeatInterval = 110 * time.Millisecond
const ApplyEntriesInterval = 200 * time.Millisecond

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
	id    string
	state ServerState

	// Persistent States
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile States
	commitIndex int
	lastApplied int

	// Leader Only States
	nextIndex  []int
	matchIndex []int

	// Etc
	electionTimer *time.Timer
	timerChan     chan bool
	applyChan     chan ApplyMsg
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
	// DPrintf("Seed: %v, State: %s", rf.me, rf.state)
	return term, isleader
}

// timer functions
func (rf *Raft) resetTimer() {
	rf.timerChan <- true
}

func (rf *Raft) checkTimeout() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()

			if rf.state == Follower {
				rf.becomeCandidate()

			} else if rf.state == Candidate {
				go rf.startElection()
			}

			rf.mu.Unlock()

		case <-rf.timerChan:
			// DPrintf("%v Does this work", rf.me)
			if !rf.electionTimer.Stop() {
				select {
				case <-rf.electionTimer.C:
				default:
				}
			}

			rf.electionTimer.Reset(electionTimeout())
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// Functions related to electing a new leader
func (rf *Raft) compareTerm(otherTerm int) {
	if otherTerm > rf.currentTerm {
		rf.becomeFollower(otherTerm)
	}
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	go rf.startElection()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = ""
	rf.resetTimer()
}

func (rf *Raft) becomeLeader() {
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

func (rf *Raft) sendHeartBeat(i int, args AppendEntriesArgs) {

	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(i, &args, &reply)

	if ok {
		// DPrintf("Sent heartbeat %v to %v", rf.me, i)
		rf.mu.Lock()
		rf.compareTerm(reply.Term)

		if rf.state == Leader {
			// DPrintf("Len Entries = %v, PrevLogIndex = %v", len(args.Entries), args.PrevLogIndex)
			rf.nextIndex[i] += len(args.Entries)
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			// DPrintf("%v has a nextIndex of %v and matchIndex of %v", i, rf.nextIndex[i], rf.matchIndex[i])
			rf.updateCommitIndex()
		}

		rf.mu.Unlock()

		if reply.ReduceNextIndex && rf.state == Leader {
			// DPrintf("Reduce %v index", i)

			args.PrevLogIndex--
			args.PrevLogTerm = -1

			rf.mu.Lock()

			args.LeaderCommit = rf.commitIndex
			// DPrintf("LeaderCommit %v", args.LeaderCommit)
			rf.nextIndex[i]--

			if args.PrevLogIndex == 0 {
				args.Entries = rf.log

			} else if rf.indexValid(args.PrevLogIndex) {
				args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
				idx := rf.nextIndex[i] - 1
				args.Entries = rf.log[idx:]
			}

			rf.mu.Unlock()

			go rf.sendHeartBeat(i, args)

		}
	}
}

// Heartbeat includes log replication
func (rf *Raft) startHeartBeat() {
	ticker := time.NewTicker(HeartbeatInterval)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	// Initial Heartbeat
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i, args)

		}
	}

	for {
		rf.mu.Lock()

		// Terminates process
		if rf.state != Leader {
			// DPrintf("%v is no longer leader", rf.me)
			ticker.Stop()
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		select {
		case <-ticker.C:

			for i := range rf.peers {

				if i != rf.me {

					rf.mu.Lock()
					args.LeaderCommit = rf.commitIndex

					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = -1
					args.Entries = []LogEntry{}

					// DPrintf("%v", args.PrevLogIndex)

					if args.PrevLogIndex == 0 {
						args.Entries = rf.log

					} else if rf.indexValid(args.PrevLogIndex) && len(rf.log) >= rf.nextIndex[i] {

						args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
						idx := rf.nextIndex[i] - 1
						args.Entries = rf.log[idx:]

					}

					rf.mu.Unlock()

					// DPrintf("LeaderCommit %v", args.LeaderCommit)

					go rf.sendHeartBeat(i, args)
				}
			}
		}
	}

}

func (rf *Raft) startElection() {
	// DPrintf("start election for: %v", rf.me)

	rf.mu.Lock()

	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.votedFor = rf.id
	rf.resetTimer()
	// rf.electionTimer.Reset(electionTimeout())

	numPeers := len(rf.peers) // This is to avoid race condition
	me := rf.me               // This is to avoid race condition

	voteChan := make(chan int, numPeers-1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		LastLogIndex: len(rf.log), // needs modification
		LastLogTerm:  0,           // needs modification
	}

	if rf.indexValid(args.LastLogIndex) {
		args.LastLogTerm = rf.getLogEntry(args.LastLogIndex).Term
	}

	rf.mu.Unlock()

	// requesting votes
	var wg sync.WaitGroup
	wg.Add(numPeers - 1)

	for i := 1; i < numPeers; i++ {
		if i != me {
			reply := RequestVoteReply{}

			go func(i int, reply RequestVoteReply) {
				// DPrintf("%v Requesting Vote from %v", rf.me, i)

				ok := rf.sendRequestVote(i, &args, &reply)

				if ok {
					if reply.VoteGranted {
						voteChan <- 1
					} else {
						voteChan <- 0

						rf.mu.Lock()
						rf.compareTerm(reply.Term)
						rf.mu.Unlock()
					}
				} else {
					voteChan <- 0
				}

				wg.Done()
			}(i, reply)
		}
	}

	voteCount := 1
	votes := 1

	for {
		select {
		case vote := <-voteChan:
			voteCount += vote
			votes += 1

			rf.mu.Lock()

			if (voteCount > (len(rf.peers) / 2)) && (rf.state == Candidate) && (rf.currentTerm == electionTerm) {
				DPrintf("%v is becoming leader", rf.me)
				rf.becomeLeader()
				rf.mu.Unlock()
				break

			} else if votes == len(rf.peers) {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}

		// time.Sleep(time.Millisecond * 10)
	}

	wg.Wait()
	close(voteChan)

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
	// Your data here (2A).
	Term        int
	CandidateId string

	// Your data here (2B).
	LastLogIndex int
	LastLogTerm  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	rf.compareTerm(args.Term)

	if rf.state == Follower {
		rf.resetTimer()
	}

	logUpToDate := func() bool {
		if len(rf.log) == 0 {
			return true
		}

		lastTerm := rf.log[len(rf.log)-1].Term
		lastIndex := len(rf.log)

		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}

		return lastTerm < args.LastLogTerm
	}()

	switch {
	case reply.Term > args.Term:
		// DPrintf("%v says no vote", rf.me)
		reply.VoteGranted = false
	case (rf.votedFor == "" || rf.votedFor == args.CandidateId) && logUpToDate:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

	default:
		// DPrintf("default case for voting")
		reply.VoteGranted = false
	}

	return
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
// Added a faster timeout thing to make it work. Could probably remove it eventually
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	// Added on
	ReduceNextIndex bool
}

// Not the best implementation a bit repetitive
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.ReduceNextIndex = false
	reply.Success = false

	switch {
	case rf.currentTerm > args.Term:
		return

	case rf.currentTerm < args.Term:
		rf.becomeFollower(args.Term)

	case rf.currentTerm == args.Term:
		switch rf.state {

		case Follower:
			rf.resetTimer()

		case Candidate:
			rf.becomeFollower(args.Term)

		}
	}

	if rf.state != Leader {
		// DPrintf("Append Entries LeaderCommit %v, My commit %v", args.LeaderCommit, rf.commitIndex)

		// DPrintf("args %v, mine %v", args.LeaderCommit, rf.commitIndex)
		if args.LeaderCommit > rf.commitIndex {
			// DPrintf("%v Update Commit Index called before: %v", rf.me, rf.commitIndex)
			rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
			// DPrintf("%v Update Commit Index called after: %v", rf.me, rf.commitIndex)
		}

		if rf.lastApplied < rf.commitIndex {
			for logIndex := rf.lastApplied + 1; logIndex < rf.commitIndex+1; logIndex++ {
				DPrintf("Follower %v, Log Index: %v, Command %v", rf.me, logIndex, rf.getLogEntry(logIndex).Command)
				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      rf.getLogEntry(logIndex).Command,
					CommandIndex: logIndex,
				}

				rf.lastApplied++
			}
		}
		// PrevLogIndex < 1 there's no logs to check.
		if args.PrevLogIndex > 0 {
			if !(rf.indexValid(args.PrevLogIndex)) {
				// DPrintf("yeet")
				reply.ReduceNextIndex = true
				return
			}

			if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
				// DPrintf("yeet")
				idx := args.PrevLogIndex - 1
				rf.log = rf.log[:(idx + 1)]
				reply.ReduceNextIndex = true
				return
			}
		}

		// DPrintf("Append Entries LeaderCommit %v, My commit %v", args.LeaderCommit, rf.commitIndex)

		// DPrintf("%v Updating log, number entries: %v, prev: %v", rf.me, len(args.Entries), args.PrevLogIndex)
		rf.updateLog(args.Entries, args.PrevLogIndex)
		// DPrintf("%v Updated log, number entries: %v", rf.me, len(rf.log))

		reply.Success = true
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		index = rf.appendLog(command)
	}

	return index, term, isLeader
}

func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		// Index:   len(rf.log),
	}

	rf.log = append(rf.log, entry)

	index = len(rf.log)

	// DPrintf("Appending, index = %v", index)
	// DPrintf("%v has a log length of %v", rf.me, len(rf.log))

	return index
}

func (rf *Raft) updateLog(entries []LogEntry, PrevLogIndex int) {

	rf.log = rf.log[:PrevLogIndex]
	rf.log = append(rf.log, entries...)

	// if len(entries) > 0 {
	// 	DPrintf("%v has a log length of %v", rf.me, len(rf.log))
	// }
	// DPrintf("%v %v: %v log length, %v prev log index", rf.state, rf.me, len(rf.log), PrevLogIndex)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// rf.electionTimer
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
		id:          string(me),
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
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

	// Store applyCh
	rf.applyChan = applyCh

	go rf.checkTimeout()
	// go rf.applyEntries()

	return rf
}

// func (rf *Raft) applyEntries() {
// 	for {
// 		rf.mu.Lock()

// 		if rf.lastApplied < rf.commitIndex {
// 			rf.lastApplied += 1
// 			index := rf.lastApplied
// 			applyMsgChan := rf.applyChan

// 			command := rf.getLogEntry(rf.lastApplied).Command

// 			rf.mu.Unlock()

// 			applyMsgChan <- ApplyMsg{
// 				CommandValid: true,
// 				Command:      command,
// 				CommandIndex: index,
// 			}

// 		} else {
// 			rf.mu.Unlock()
// 			time.Sleep(ApplyEntriesInterval)

// 		}
// 	}
// }

func (rf *Raft) getLogEntry(index int) LogEntry {
	idx := index - 1
	return rf.log[idx]
}

func (rf *Raft) indexValid(index int) bool {
	if index == 0 {
		return false
	}

	return (index - 1) < len(rf.log)
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (rf *Raft) updateCommitIndex() {

	// DPrintf("%v Update Commit Index called before: %v", rf.me, rf.commitIndex)
	majority := len(rf.peers) / 2
	prevCommitIndex := rf.commitIndex

	for logIndex := rf.commitIndex + 1; logIndex < len(rf.log)+1; logIndex++ {
		count := 1

		for i := range rf.peers {
			if i != rf.me {
				if rf.matchIndex[i] >= logIndex {
					count++
				}
			}
		}

		if (count > majority) && (rf.getLogEntry(logIndex).Term == rf.currentTerm) {
			rf.commitIndex = logIndex
		}
	}

	for toCommit := prevCommitIndex + 1; toCommit < rf.commitIndex+1; toCommit++ {
		DPrintf("Leader %v, Log Index: %v, Command %v, nextIndex for 0: %v, nextIndex for 1: %v, nextIndex for 2: %v", rf.me, toCommit, rf.getLogEntry(toCommit).Command, rf.nextIndex[0], rf.nextIndex[1], rf.nextIndex[2])
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(toCommit).Command,
			CommandIndex: toCommit,
		}
	}

	// DPrintf("%v Update Commit Index called after: %v", rf.me, rf.commitIndex)
}
