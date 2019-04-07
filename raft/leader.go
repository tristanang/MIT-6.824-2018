package raft

import (
	"time"
)

const HeartbeatInterval = time.Duration(100 * time.Millisecond)

// Periodic heart beats that leader uses to maintain log consistency and inform
// followers of leadership
func (rf *Raft) startHeartbeat() {

	// Can't get ticker to work so bad timer will have to do.
	timer := time.NewTimer(HeartbeatInterval)

	for {
		select {
		case <-rf.kill:
			return

		case <-timer.C:
			rf.mu.Lock()
			myState := rf.state
			rf.mu.Unlock()

			if myState != Leader {
				return
			}

			go rf.pushLog()
			timer.Reset(HeartbeatInterval)
		}
	}
}

// Starts concurrent processes for log consensus
func (rf *Raft) pushLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.pushLogHelper(i)
		}
	}
}

func (rf *Raft) pushLogHelper(server int) {
	rf.mu.Lock()

	// server is too far behind and snapshot needs to be sent.
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		go rf.sendSnapshot(server)
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.getEntry(prevLogIndex).Term

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex}

	if rf.nextIndex[server] < rf.logIndex {
		entries := rf.getLogSlice(prevLogIndex+1, rf.logIndex)
		args.Entries = entries
	}

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Success {
			prevLogIndex := args.PrevLogIndex
			if prevLogIndex+len(args.Entries) >= rf.nextIndex[server] {
				rf.nextIndex[server] = prevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = prevLogIndex + len(args.Entries)
			}

			toCommitIndex := prevLogIndex + len(args.Entries)

			if rf.commitCheck(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				rf.persist()
				rf.applySignal <- true
			}

		} else {
			if reply.Term > rf.currentTerm {
				// Leader loses authorithy
				rf.demoteLeader(reply.Term)

			} else {
				// follower has log conflict.
				rf.nextIndex[server] = Min(reply.ConflictIndex, rf.logIndex)
				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					go rf.sendSnapshot(server)
				}
			}
		}
	}
}

func (rf *Raft) commitCheck(index int) bool {
	// We are in the correct term
	if rf.getEntry(index).Term == rf.currentTerm &&
		// out of bounds error
		index < rf.logIndex &&
		// no point checking if for things already commited
		rf.commitIndex < index {
		count := 0

		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				count += 1
			}
		}
		return count > len(rf.peers)/2

	}
	return false
}
