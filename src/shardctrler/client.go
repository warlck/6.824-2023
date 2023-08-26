package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers           []*labrpc.ClientEnd
	clientID          int64
	requestSequenceNo int64
	mu                sync.Mutex
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
	ck.clientID = nrand()
	ck.requestSequenceNo = 1
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {

	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := &QueryArgs{
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	Debug(dClient, "Clerk: %d is calling  Join  servers: %+v", ck.clientID, servers)

	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := &JoinArgs{
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}
	// Your code here.
	args.Servers = servers
	Debug(dClient, "Clerk: %d is calling  Join  args: %+v", ck.clientID, args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := &LeaveArgs{
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := &MoveArgs{
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}

	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
