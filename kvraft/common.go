package raftkv

const (
	OK           = "OK"
	ErrNotLeader = "ErrNotLeader"
	ErrNoKey     = "ErrNoKey"
	ErrGeneric   = "Generic Error"
)

type Err string

type PutAppendArgs struct {
	ServerId  int64
	RequestId int

	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	
}

type PutAppendReply struct {
	// WrongLeader bool // Unfortunately, it is better to just assume it is the wrong leader when rpc fails
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	// WrongLeader bool
	Err         Err
	Value       string
}

type RequestArgs struct {
	Term  int
	Value string
	Err   Err
}