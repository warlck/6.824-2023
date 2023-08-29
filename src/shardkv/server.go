package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	Key          string
	Value        string
	Op           string
	RequestSeqID int64
	ClientID     int64
}

type OpResponse struct {
	RequestSeqID int64
	Index        int
	ClientID     int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Represents in memory KV store
	stateMachine map[string]string
	// Stores Channeels that RPC hanlders are waiting on
	opResponseWaiters map[int]chan OpResponse

	// Duplicate table is used to prevent processing duplicate Put/Append/Get requests
	// sent by Clerk
	duplicateTable map[int64]OpResponse

	sm          *shardctrler.Clerk
	shardConfig shardctrler.Config

	// Need to maintain the dead/alive state so that long living
	// goroutines and channels can be cleared
	dead int32
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	err, ret := kv.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	if !kv.shardIsServedByGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	command := Op{
		Op:           Get,
		Key:          args.Key,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	kv.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)

	kv.mu.Lock()
	reply.Value = kv.stateMachine[args.Key]
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	err, ret := kv.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	if !kv.shardIsServedByGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	command := Op{
		Op:           args.Op,
		Key:          args.Key,
		Value:        args.Value,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	kv.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// Your initialization code here.
	kv.stateMachine = make(map[string]string)
	kv.duplicateTable = make(map[int64]OpResponse)
	kv.opResponseWaiters = make(map[int]chan OpResponse)

	// Get the latest shard configuration before completing shardKV
	// initialization
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardConfig = kv.sm.Query(-1)

	go kv.fetchLatestShardConfig()

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.receiveApplyMessages()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.installStateFromSnapshot(persister.ReadSnapshot())
	return kv
}

func (kv *ShardKV) fetchLatestShardConfig() {
	for {
		if kv.killed() {
			return
		}

		// ask controler for the latest configuration.
		kv.mu.Lock()
		kv.shardConfig = kv.sm.Query(-1)
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

}

// Processes incoming ApplyMsg values that were processed and passed from Raft.
// The main task of the method is to wake up the RPC handler that is waiting on applied message
// at particular log index
func (kv *ShardKV) receiveApplyMessages() {
	for {
		if kv.killed() {
			return
		}

		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if ok {
				dupTableEntry := kv.duplicateTable[op.ClientID]
				response := OpResponse{
					RequestSeqID: op.RequestSeqID,
					ClientID:     op.ClientID,
					Index:        applyMsg.CommandIndex,
				}

				Debug(dCommit, "S%d received message: %+v, response: %+v ", kv.me, applyMsg, dupTableEntry)
				if op.RequestSeqID > dupTableEntry.RequestSeqID {
					kv.duplicateTable[op.ClientID] = response
					kv.applyOpToStateMachineL(op)
					kv.snapShotKVState(applyMsg.CommandIndex)
				}

				reponseWaiter, ok := kv.opResponseWaiters[applyMsg.CommandIndex]
				// RPC handler has created a channel that it is waiting on before replying to client request
				if ok {
					// Wakes up the RPC handler that is waiting on this channel.
					// This is safe, as the channel needs to be buffered.
					reponseWaiter <- response
				} else {
					// If RPC handler was late to create a channel,  create a buffered channel
					// for RPC handler to eventually read and reply to client
					bufferedWaiter := make(chan OpResponse, 1)
					bufferedWaiter <- response
					kv.opResponseWaiters[applyMsg.CommandIndex] = bufferedWaiter
				}

			}
		} else if applyMsg.SnapshotValid {
			kv.installStateFromSnapshot(applyMsg.Snapshot)
		}
		// Debug(dCommit, "S%d applied message SM:%+v", kv.me, kv.stateMachine)

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyOpToStateMachineL(op Op) {
	if op.Op == "Put" {
		kv.stateMachine[op.Key] = op.Value
	}

	if op.Op == "Append" {
		value, ok := kv.stateMachine[op.Key]
		if ok {
			kv.stateMachine[op.Key] = fmt.Sprintf("%s%s", value, op.Value)
		} else {
			kv.stateMachine[op.Key] = op.Value
		}
	}
}

func (kv *ShardKV) snapShotKVState(index int) {
	if kv.maxraftstate < 0 {
		return
	}

	if len(kv.persister.ReadRaftState()) >= 9*kv.maxraftstate/10 {
		data := kv.snapshotData()
		kv.rf.Snapshot(index, data)
	}
}

func (kv *ShardKV) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.duplicateTable)
	return w.Bytes()
}

func (kv *ShardKV) installStateFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stateMachine map[string]string
	var duplicateTable map[int64]OpResponse
	var err error

	if err := d.Decode(&stateMachine); err != nil {
		log.Fatal("Failed to read log from persistent state", err)
	}

	if err = d.Decode(&duplicateTable); err != nil {
		log.Fatal("Failed to read votedFor from persistent state", err)
	}

	kv.stateMachine = stateMachine
	kv.duplicateTable = duplicateTable
}
