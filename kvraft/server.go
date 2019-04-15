package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	// "log"
	"raft"
	"sync"
	"time"
)

const NotifyTimeout = time.Duration(3 * time.Second)

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister *raft.Persister // Unfortunately cannot read raft persister

	// Channels
	kill chan bool

	// Maps
	dict          map[string]string       // kv pairs
	processed     map[int64]int           // processed put/append requests
	requestMap    map[int]chan RequestArgs // use log index as key
}


func (kv *KVServer) persist(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.processed)
	e.Encode(kv.dict)
	snapshot := w.Bytes()
	kv.rf.PersistAndSaveSnapshot(lastCommandIndex, snapshot)
}


func (kv *KVServer) snapshotCheck(lastCommandIndex int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.persist(lastCommandIndex)
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, term, isLeader := kv.rf.Start(args.copy())

	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	requestCh := make(chan RequestArgs, 1)

	kv.mu.Lock()
	kv.requestMap[index] = requestCh
	kv.mu.Unlock()

	select {
	case <-time.After(NotifyTimeout):
		kv.mu.Lock()
		delete(kv.requestMap, index)
		kv.mu.Unlock()

		reply.Err = ErrGeneric
		return

	case result := <-requestCh:
		if result.Term != term {
			reply.Err = ErrNotLeader
			return

		// success
		} else {
			reply.Err = result.Err
			reply.Value = result.Value
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	index, term, isLeader := kv.rf.Start(args.copy())

	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	requestCh := make(chan RequestArgs, 1)
	
	kv.mu.Lock()
	kv.requestMap[index] = requestCh
	kv.mu.Unlock()

	select {
	case <-time.After(NotifyTimeout):
		kv.mu.Lock()
		delete(kv.requestMap, index)
		kv.mu.Unlock()

		reply.Err = ErrGeneric
		return

	case result := <-requestCh:
		if result.Term != term {
			reply.Err = ErrNotLeader
			return
			
		} else {
			reply.Err = result.Err
			return
		}
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.kill)
	// Your code here, if desired.
}


func (kv *KVServer) apply(msg raft.ApplyMsg) {
	response := RequestArgs{
		Term:  msg.CommandTerm,
		Value: "",
		Err:   OK}

	// if command is a Get
	if args, ok := msg.Command.(GetArgs); ok {
		response.Value = kv.dict[args.Key]

		// if command is a PutAppend
	} else if args, ok := msg.Command.(PutAppendArgs); ok {
		// if this boolean is not satisfied, we need to wait on other requests
		if kv.processed[args.ServerId] < args.RequestId {
			kv.applyPutAppendHelper(args)
		}

	} else {
		// Generic error (Technically we don't know)
		response.Err = ErrGeneric
	}
	kv.requestPush(msg.CommandIndex, response)
	kv.snapshotCheck(msg.CommandIndex)
}


func (kv *KVServer) applyPutAppendHelper(args PutAppendArgs) {
	if args.Op == "Put" {
		kv.dict[args.Key] = args.Value

		// else it's an append
	} else {
		kv.dict[args.Key] += args.Value
	}
	kv.processed[args.ServerId] = args.RequestId
	return
}


func (kv *KVServer) listen() {
	go kv.rf.Replay()
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()

			if msg.CommandValid {
				kv.apply(msg)
			}

			kv.mu.Unlock()

		case <-kv.kill:
			return
		}
	}
}


func (kv *KVServer) requestPush(index int, result RequestArgs) {
	ch, ok := kv.requestMap[index]

	if ok {
		// the key is done
		delete(kv.requestMap, index)
		ch <- result
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant Key/Value service.
// me is the Index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.maxraftstate = maxraftstate
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister // unfortunately cannot call on kv.rf.persister
	
	kv.kill = make(chan bool)

	kv.dict = make(map[string]string)
	kv.processed = make(map[int64]int)
	kv.requestMap = make(map[int]chan RequestArgs)

	go kv.listen()

	return kv
}
