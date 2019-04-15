package raft 

import (
	"labgob"
	crand "crypto/rand"
	"math/big"
	"math/rand"
	"log"
)

func (rf *Raft) Replay() {
	rf.mu.Lock()
	start := 1

	if start <= rf.lastIncludedIndex {
		rf.applyCh <- ApplyMsg{
			CommandValid: false, 
			CommandIndex: rf.log[0].Index, 
			CommandTerm: rf.log[0].Term, 
			Command: "InstallSnapshot"}

		start = rf.lastIncludedIndex + 1

		if rf.lastIncludedIndex > rf.lastApplied {
			rf.lastApplied = rf.lastIncludedIndex
		}
	}

	entries := append([]LogEntry{}, rf.log[rf.getOffset(start):rf.getOffset(rf.lastApplied+1)]...)
	rf.mu.Unlock()

	for i := 0; i < len(entries); i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true, 
			CommandIndex: entries[i].Index, 
			CommandTerm: entries[i].Term, 
			Command: entries[i].Command}
	}
}

// seed random number generator
func init() {
	labgob.Register(LogEntry{})
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	seed := bigx.Int64()
	rand.Seed(seed)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func (rf *Raft) PersistAndSaveSnapshot(lastIncludedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex > rf.lastIncludedIndex {
		// truncate log
		rf.log = append([]LogEntry{}, rf.log[rf.getOffset(lastIncludedIndex):]...) // log entry previous at lastIncludedIndex at 0 now
		rf.lastIncludedIndex = lastIncludedIndex
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}