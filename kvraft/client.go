package raftkv

import (
	"labrpc"
	"math/big"
	"time"
	"crypto/rand"
)

const RetrySleep = time.Duration(125 * time.Millisecond)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clerkId   int64
	RequestId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.RequestId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				return reply.Value
			}
		}

		ck.leaderId++
		ck.leaderId %= len(ck.servers)

		time.Sleep(RetrySleep)
	}

	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.RequestId++
	args := PutAppendArgs{ck.clerkId, ck.RequestId, key, value, op}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				return
			}
		}

		ck.leaderId++
		ck.leaderId %= len(ck.servers)

		time.Sleep(RetrySleep)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
