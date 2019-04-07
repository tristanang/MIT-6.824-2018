// Design Notes:
// 1) As much as possible, DO NOT use mutex in helper functions.
// 2) Encapsulate indexing.

package raft

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"sync"
	"time"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// General States
	state    ServerState
	leaderId int

	// Persistent States
	currentTerm int
	votedFor    int
	log         []LogEntry
	logIndex    int

	// Volatile States
	commitIndex int
	lastApplied int

	// Leader Only States
	nextIndex  []int
	matchIndex []int

	// Timers
	electionTimer *time.Timer

	// kill
	kill chan bool

	// applying
	applyCh     chan ApplyMsg
	applySignal chan bool

	// snapshotting
	lastIncludedIndex int // index of the last entry in the log that snapshot replaces
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() []byte {
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// persistent states
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	// others
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)

	// volatile (not supposed to encode but doesn't work otherwise)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)

	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, lastIncludedIndex, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0, 0

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		log.Fatal("Error in decoding")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.logIndex = logIndex
	rf.commitIndex = commitIndex
	rf.lastApplied = lastApplied
}

// snapshot truncates the log data structure. Thus, the array index no longer
// corresponds to the LogIndex
func (rf *Raft) getOffset(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffset(i)
	return rf.log[offsetIndex]
}

// half open interval by convention
func (rf *Raft) getLogSlice(startIndex int, endIndex int) []LogEntry {
	startIndex2 := rf.getOffset(startIndex)
	endIndex2 := rf.getOffset(endIndex)
	return append([]LogEntry{}, rf.log[startIndex2:endIndex2]...)
}

// pushing snapshot to followers
func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.getEntry(rf.lastIncludedIndex).Term,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm {
			rf.demoteLeader(reply.Term)
		} else {
			rf.nextIndex[server] = Max(rf.nextIndex[server], rf.lastIncludedIndex+1)
			rf.matchIndex[server] = Max(rf.matchIndex[server], rf.lastIncludedIndex)
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader bool
	// Your code here (2A).

	term, state := rf.GetStateHelper()
	isleader = (state == Leader)

	return term, isleader
}

// Better GetState (mostly defunct)
func (rf *Raft) GetStateHelper() (int, ServerState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.state == Leader)

	// If server is not leader, we cannot update log.
	if isLeader {

		term = rf.currentTerm
		index = rf.logIndex

		entry := LogEntry{
			Index:   index,
			Term:    rf.currentTerm,
			Command: command,
		}

		offsetIndex := rf.getOffset(rf.logIndex)

		if offsetIndex < len(rf.log) {
			rf.log[offsetIndex] = entry
		} else {
			rf.log = append(rf.log, entry)
		}

		rf.matchIndex[rf.me] = rf.logIndex
		rf.logIndex += 1
		rf.persist()

		// It isn't necessary to push logs when start. We can just rely on heartbeats
		// However, tests run faster when we include this.
		go rf.pushLog()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	// For some reason including the kill switch reduces liveliness
	// rf.kill <- true
	close(rf.kill)
}

// listening go routine for apply.
func (rf *Raft) applyListen() {

	for {
		select {
		case <-rf.applySignal:

			var commandValid bool
			var entries []LogEntry

			rf.mu.Lock()

			// We apply everything that has been snapshotted
			if rf.lastApplied < rf.lastIncludedIndex {
				commandValid = false
				rf.lastApplied = rf.lastIncludedIndex
				entries = []LogEntry{{Index: rf.lastIncludedIndex, Term: rf.log[0].Term, Command: "InstallSnapshot"}}

				// Apply everything up to commitIndex
			} else if rf.lastApplied < rf.logIndex &&
				rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getLogSlice(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}

			rf.persist()
			rf.mu.Unlock()

			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: commandValid,
					CommandIndex: entry.Index,
					CommandTerm:  entry.Term,
					Command:      entry.Command,
				}
			}

		case <-rf.kill:
			return
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		//
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		leaderId:    -1,
		//
		commitIndex: 0,
		lastApplied: 0,
		//
		electionTimer: time.NewTimer(randTimeout()),
		kill:          make(chan bool),
		applyCh:       applyCh,
		applySignal:   make(chan bool, 10),
		//
		log:               []LogEntry{{0, 0, nil}}, // To make indexing easier, create dummy entry at index 0
		logIndex:          1,
		lastIncludedIndex: 0,
	}

	// Your initialization code here (2A, 2B, 2C).

	rf.readPersist() // initialize from state persisted before a crash

	go rf.applyListen()

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()

				if rf.state == Leader {
					rf.mu.Unlock()
					continue
				}

				rf.mu.Unlock()

				go rf.startElection() // follower timeout, start a new election

			case <-rf.kill:
				return
			}
		}
	}()

	return rf
}

// AppendEntries RPC as described in paper
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// follower does not acknowledge leadership
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	// Leadership acknowledged
	rf.leaderId = args.LeaderId
	rf.resetTimer()
	rf.state = Follower

	rf.compareTerm(args.Term)

	prevLogIndex := args.PrevLogIndex

	if prevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	// first part prevents getEntry out of bounds error
	if rf.logIndex <= prevLogIndex ||
		// follower don't agree with leader on last log entry. 5.3
		rf.getEntry(prevLogIndex).Term != args.PrevLogTerm {

		reply.Success = false
		reply.ConflictIndex = rf.findLatestConflict(prevLogIndex)
		return
	}

	reply.Success, reply.ConflictIndex = true, -1

	rf.updateLogHelper(prevLogIndex, args)

	return
}

func (rf *Raft) findLatestConflict(prevLogIndex int) int {
	conflictIndex := Min(rf.logIndex-1, prevLogIndex)
	conflictTerm := rf.getEntry(conflictIndex).Term

	lastestCorrectIndex := Max(rf.lastIncludedIndex, rf.commitIndex)

	for conflictIndex > lastestCorrectIndex && rf.getEntry(conflictIndex-1).Term == conflictTerm {
		conflictIndex--
	}

	return conflictIndex
}

func (rf *Raft) updateLogHelper(prevLogIndex int, args *AppendEntriesArgs) {
	index := rf.getIndexToAppend(prevLogIndex, args)

	for i := index; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}

	flag := false

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		flag = true
	}

	rf.persist()
	rf.resetTimer()

	if flag {
		rf.applySignal <- true
	}
}

func (rf *Raft) getIndexToAppend(prevLogIndex int, args *AppendEntriesArgs) int {
	i := 0

	for i < len(args.Entries) && prevLogIndex+1+i < rf.logIndex {

		if rf.getEntry(prevLogIndex+1+i).Term != args.Entries[i].Term {
			// 5.3. Point 3 on Pg 4
			rf.logIndex = prevLogIndex + 1 + i
			rf.log = rf.log[:rf.getOffset(rf.logIndex)]
			return i
		}

		i++
	}

	return i
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.leaderId = args.LeaderId

	// Our snapshot is outdated.
	if args.LastIncludedIndex > rf.lastIncludedIndex {

		rf.lastIncludedIndex = args.LastIncludedIndex
		flag := false

		if rf.lastIncludedIndex < rf.commitIndex {
			rf.commitIndex = rf.lastIncludedIndex
			flag = true
		}

		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)

		overwriteIndex := rf.getOffset(args.LastIncludedIndex)

		// snapshot is subset of log
		if overwriteIndex < len(rf.log) {
			rf.log = rf.log[overwriteIndex:]

			// snapshot is prefix of log
		} else {
			// delete log leaving only snapshot residue
			rf.log = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}}
		}

		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if flag {
			rf.applySignal <- true
		}
	}

	rf.resetTimer()
	rf.persist()
}
