package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrStaleRequest = "ErrStaleRequest"
	ErrWrongConfig  = "ErrWrongConfig"

	Get    = "Get"
	Put    = "Put"
	Append = "Append"
	Config = "Config"
)

type Err string

type Replier interface {
	ReplyOK()
	ReplyWrongLeader()
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID     int64
	RequestSeqID int64
}

type PutAppendReply struct {
	Err Err
}

func (p *PutAppendReply) ReplyOK() {
	p.Err = OK
}

func (p *PutAppendReply) ReplyWrongLeader() {
	p.Err = ErrWrongLeader
}

type GetArgs struct {
	Key string

	ClientID     int64
	RequestSeqID int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (g *GetReply) ReplyOK() {
	g.Err = OK
}

func (g *GetReply) ReplyWrongLeader() {
	g.Err = ErrWrongLeader
}

type GetShardArgs struct {
	ShardID   int
	ConfigNum int
}

type GetShardReply struct {
	Err      Err
	ShardKVs map[string]string
}
