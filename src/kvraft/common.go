package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrStaleRequest = "ErrStaleRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	RequestSeqID int64
	ClientID     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	RequestSeqID int64
	ClientID     int64
}

type GetReply struct {
	Err   Err
	Value string
}
