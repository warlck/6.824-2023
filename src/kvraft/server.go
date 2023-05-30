package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	Key          string
	Value        string
	Op           string
	RequestSeqID int64
	ClientID     int64
}

type OpResponse struct {
	requestSeqID int64
	index        int
	clientID     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Represents in memory KV store
	stateMachine map[string]string
	// Stores Channeels that RPC hanlders are waiting on
	opResponseWaiters map[int]chan OpResponse

	// Duplicate table is used to prevent processing duplicate Put/Append/Get requests
	// sent by Clerk
	duplicateTable map[int64]OpResponse

	// Channel to receives a signal whenever Raft's term has changed
	termChangedCh chan int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Key:          args.Key,
		Op:           "Get",
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	reply.ServerID = kv.me
	index, _, isLeader := kv.rf.Start(command)
	//Debug(dClient, "S%d received Get  Request | before Lock %+v, isLeader:%t, index: %d", kv.me, kv.stateMachine, isLeader, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dClient, "S%d received Get  Request | before lock, args: %+v, index: %d, isLeader: %t",
		kv.me, args, index, isLeader)
	kv.mu.Lock()
	Debug(dClient, "S%d received Get  Request | after lock", kv.me)
	// We need to check if ApplyMsg receiver has  already received an apply message with current index
	// If the apply message with current index already has been received, responseWaiter will contain a buffered channel with
	// buffer of 1. We can read the value in the buffered channel and return to client
	responseWaiter, ok := kv.opResponseWaiters[index]

	// If apply message with current index has not been processsed yet, create a channel that we use to wait
	// for  Raft processing to complete.
	if !ok {
		responseWaiter = make(chan OpResponse, 1)
		kv.opResponseWaiters[index] = responseWaiter
	}
	reply.Value = kv.stateMachine[args.Key]
	kv.mu.Unlock()

	opResponse := <-responseWaiter
	// Clear the channel from the Server's reponsewaiters
	go kv.removeResponseWaiter(index)
	// If the command that Raft applied, matches the command that this RPC handlder has submitted
	// (i.e RequestUUID and index matches)
	// the request has been sucessfully commited to stateMachine.
	// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
	if opResponse.index == index && opResponse.requestSeqID == args.RequestSeqID && opResponse.clientID == args.ClientID {
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	response := kv.duplicateTable[args.ClientID]
	kv.mu.Unlock()

	Debug(dClient, "S%d received PutAppend | before lock, reponse: %+v, args: %+v", kv.me, response, args)

	reply.ServerID = kv.me
	if response.requestSeqID == args.RequestSeqID && response.clientID == args.ClientID {
		reply.Err = OK
		return
	}

	if args.RequestSeqID < response.requestSeqID && response.clientID == args.ClientID {
		reply.Err = ErrStaleRequest
		return
	}

	command := Op{
		Key:          args.Key,
		Value:        args.Value,
		Op:           args.Op,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	index, _, isLeader := kv.rf.Start(command)
	// Debug(dClient, "S%d received PutAppend | before lock, index: %d, isLeader: %t ", kv.me, index, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	Debug(dClient, "S%d received PutAppend | after lock ", kv.me)
	// We need to check if ApplyMsg receiver has  already received an apply message with current index
	// If the apply message with current index already has been received, responseWaiter will contain a buffered channel with
	// buffer of 1. We can read the value in the buffered channel and return to client
	responseWaiter, ok := kv.opResponseWaiters[index]

	// If apply message with current index has not been processsed yet, create a channel that we use to wait
	// for  Raft processing to complete.
	if !ok {
		responseWaiter = make(chan OpResponse, 1)
		kv.opResponseWaiters[index] = responseWaiter
	}
	kv.mu.Unlock()

	opResponse := <-responseWaiter
	// Clear the channel from the reponsewaiters map
	go kv.removeResponseWaiter(index)
	// If the command that Raft applied, matches the command that this RPC handlder has submitted
	// (i.e RequestUUID and index matches)
	// the request has been sucessfully commited to stateMachine.
	// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
	if opResponse.index == index && opResponse.requestSeqID == args.RequestSeqID && opResponse.clientID == args.ClientID {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.receiveApplyMessages()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = make(map[string]string)
	kv.opResponseWaiters = make(map[int]chan OpResponse)
	kv.duplicateTable = make(map[int64]OpResponse)

	return kv
}

// Processes incoming ApplyMsg values that were processed and passed from Raft.
// The main task of the method is to wake up the RPC handler that is waiting on applied message
// at particular log index
func (kv *KVServer) receiveApplyMessages() {
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()
		Debug(dCommit, "S%d received message %+v", kv.me, applyMsg)
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if ok {
				kv.applyOpToStateMachineL(op)
				reponseWaiter, ok := kv.opResponseWaiters[applyMsg.CommandIndex]
				reponse := OpResponse{
					requestSeqID: op.RequestSeqID,
					clientID:     op.ClientID,
					index:        applyMsg.CommandIndex,
				}
				kv.duplicateTable[op.ClientID] = reponse
				// RPC handler has created a channel that it is waiting on before replying to client request
				if ok {
					// Wakes up the RPC handler that is waiting on this channel.
					// This is safe, as the channel needs to be buffered.
					reponseWaiter <- reponse
				} else {
					// If RPC handler was late to create a channel,  create a buffered channel
					// for RPC handler to eventually read and reply to client
					bufferedWaiter := make(chan OpResponse, 1)
					bufferedWaiter <- reponse
					kv.opResponseWaiters[applyMsg.CommandIndex] = bufferedWaiter
				}
			}
		}
		//	Debug(dCommit, "S%d applied message SM:%+v", kv.me, kv.stateMachine)

		kv.mu.Unlock()
	}
}

func (kv *KVServer) applyOpToStateMachineL(op Op) {
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

// Clears the response waiter channel that was used to by RPC KVServer's RPC handlers
func (kv *KVServer) removeResponseWaiter(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.opResponseWaiters, index)
}
