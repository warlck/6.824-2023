package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers           []*labrpc.ClientEnd
	lastServer        int
	clientID          int64
	requestSequenceNo int64
	mu                sync.Mutex
	// You will have to modify this struct.
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
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := GetArgs{
		Key:          key,
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}

	reply := GetReply{}
	var i = ck.lastServer
	for ; i < len(ck.servers); i++ {
		Debug(dClient, "Clerk: %d is calling Get on server: %d, args: %+v", ck.clientID, i, args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		Debug(dClient, "Clerk: %d has received reply  for  Get from  server: %d, OK: %t,  args: %+v,  reply {Err: %s, ServerID: %d}",
			ck.clientID, i, ok, args, reply.Err, reply.ServerID)

		if ok && reply.Err == OK {
			ck.lastServer = i
			return reply.Value
		} else {
			// For now  request tries another server from the list
			// TODO: Build optimization that will receive a LeaderID on ErrWrongLeader
			// will make quuery to Leader

			// If client has already looped through all the servers and did not find the Leader, need to start loop again
			if i == len(ck.servers)-1 {
				i = -1
			}
		}
	}

	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	requestSequenceNumber := ck.requestSequenceNo
	ck.requestSequenceNo++
	ck.mu.Unlock()

	args := PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		RequestSeqID: requestSequenceNumber,
		ClientID:     ck.clientID,
	}

	reply := PutAppendReply{}

	var i = ck.lastServer
	for ; i < len(ck.servers); i++ {
		Debug(dClient, "Clerk: %d is calling  PutAppend on  server: %d, args: %+v", ck.clientID, i, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		Debug(dClient, "Clerk: %d has received reply  for  PutAppend from server: %d, OK: %t,  args: %+v, reply: %+v",
			ck.clientID, i, ok, args, reply)

		if ok && reply.Err == OK {
			ck.lastServer = i
			return
		} else {
			// The RPC request is stale, no need to continue
			if reply.Err == ErrStaleRequest {
				return
			}
			// For now  request tries another server from the list
			// TODO: Build optimization that will receive a LeaderID on ErrWrongLeader
			// will make quuery to Leader

			// If client has already looped through all the servers and did not find the Leader, need to start loop again
			if i == len(ck.servers)-1 {
				i = -1
			}
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
