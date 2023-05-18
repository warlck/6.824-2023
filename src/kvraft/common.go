package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	RequestUUID string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key         string
	RequestUUID string
}

type GetReply struct {
	Err   Err
	Value string
}
