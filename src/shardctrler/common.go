package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK              = "OK"
	ErrStaleRequest = "ErrStaleRequest"
	ErrWrongLeader  = "ErrWrongLeader"

	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Replier interface {
	ReplyOK()
	ReplyWrongLeader()
}

type Err string

type JoinArgs struct {
	Servers      map[int][]string // new GID -> servers mappings
	ClientID     int64
	RequestSeqID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

func (j *JoinReply) ReplyOK() {
	j.Err = OK
}

func (j *JoinReply) ReplyWrongLeader() {
	j.Err = ErrWrongLeader
	j.WrongLeader = true
}

type LeaveArgs struct {
	GIDs         []int
	ClientID     int64
	RequestSeqID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

func (l *LeaveReply) ReplyOK() {
	l.Err = OK
}
func (l *LeaveReply) ReplyWrongLeader() {
	l.Err = ErrWrongLeader
	l.WrongLeader = true
}

type MoveArgs struct {
	Shard        int
	GID          int
	ClientID     int64
	RequestSeqID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

func (m *MoveReply) ReplyOK() {
	m.Err = OK
}
func (m *MoveReply) ReplyWrongLeader() {
	m.Err = ErrWrongLeader
	m.WrongLeader = true
}

type QueryArgs struct {
	Num          int // desired config number
	ClientID     int64
	RequestSeqID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (q *QueryReply) ReplyOK() {
	q.Err = OK
}
func (q *QueryReply) ReplyWrongLeader() {
	q.Err = ErrWrongLeader
	q.WrongLeader = true
}
