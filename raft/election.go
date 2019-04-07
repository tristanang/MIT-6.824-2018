package raft

import (
	"math/rand"
	"time"
)

const ElectionTimeout = 1000

// Timer functions

// generate random time duration
func randTimeout() time.Duration {
	return time.Duration(ElectionTimeout+rand.Intn(ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) resetTimer() {
	// https://golang.org/pkg/time/#Timer.Reset
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randTimeout())
}

// state transition functions

func (rf *Raft) compareTerm(otherTerm int) {

	if otherTerm > rf.currentTerm {

		if rf.state != Follower {
			rf.becomeFollower(otherTerm)

		} else {
			rf.currentTerm = otherTerm
			rf.votedFor = -1
		}
	}
}

// Wrapper for becomeFollower followed by persist.
func (rf *Raft) demoteLeader(term int) {
	rf.becomeFollower(term)
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetTimer()
}

func (rf *Raft) becomeLeader() {
	// DPrintf("%v is becoming the leader.", rf.me)

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		// if != me... somehow we need to remove it
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0 // initialized to 0
	}

	go rf.startHeartbeat()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// End this request quick without persisting
	if reply.Term == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		return
	}

	// Note: rf.compareTerm within voteCheck. Weird but cleaner.
	if !rf.voteCheck(args) {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetTimer()
	rf.persist()
}

func (rf *Raft) voteCheck(args *RequestVoteArgs) bool {
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		return false
	}

	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.getEntry(lastLogIndex).Term

	// Since our term is smaller or equal to the other term, we compare.
	rf.compareTerm(args.Term)

	// the server has log with higher term
	if lastLogTerm > args.LastLogTerm ||
		// Additional safety requirement as highlighted
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return false
	}

	return true
}

func (rf *Raft) startElection() {
	// Unfortunately, messy mutexes because a lot of things happen here.

	rf.mu.Lock()

	// Updating internal states given start of election
	rf.leaderId = -1
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me

	me := rf.me               // To prevent race condition
	numPeers := len(rf.peers) // To prevent race condition

	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.getEntry(lastLogIndex).Term

	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.resetTimer()

	rf.mu.Unlock()

	voteChan := make(chan RequestVoteReply, numPeers-1)

	for i := 0; i < numPeers; i++ {
		if i != me {
			go rf.askVote(i, args, voteChan)
		}
	}

	voteCount, threshold := 1, numPeers/2

	for voteCount <= threshold {
		select {
		case <-rf.kill:
			return

		case <-rf.electionTimer.C: // election timeout
			return

		case reply := <-voteChan:
			if reply.VoteGranted {
				voteCount += 1

			} else {
				// If vote is not granted there is possibility term is outdated

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					break
				}

				rf.mu.Unlock()

			}
		}
	}

	rf.mu.Lock()

	// Only Candidates are allowed to be leaders
	if rf.state == Candidate {
		// If we are here then we have won the election
		rf.becomeLeader()
	}

	rf.mu.Unlock()

	// No return here... for reasons... (leave channel open)
}

func (rf *Raft) askVote(i int, args RequestVoteArgs, voteChan chan RequestVoteReply) {
	reply := RequestVoteReply{}
	rf.sendRequestVote(i, &args, &reply)
	voteChan <- reply
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
