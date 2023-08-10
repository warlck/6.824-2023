package kvraft

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

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Represents in memory KV store
	stateMachine map[string]string
	// Stores Channeels that RPC hanlders are waiting on
	opResponseWaiters map[int]chan OpResponse

	// Duplicate table is used to prevent processing duplicate Put/Append/Get requests
	// sent by Clerk
	duplicateTable map[int64]OpResponse
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	response := kv.duplicateTable[args.ClientID]
	reply.Value = kv.stateMachine[args.Key]
	kv.mu.Unlock()

	Debug(dClient, "S%d received Get | before lock, reponse: %+v, args: %+v", kv.me, response, args)

	reply.ServerID = kv.me
	if response.RequestSeqID == args.RequestSeqID && response.ClientID == args.ClientID {
		reply.Err = OK
		return
	}

	if args.RequestSeqID < response.RequestSeqID && response.ClientID == args.ClientID {
		reply.Err = ErrStaleRequest
		reply.Value = ""
		return
	}

	command := Op{
		Key:          args.Key,
		Op:           "Get",
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	index, term, isLeader := kv.rf.Start(command)
	//Debug(dClient, "S%d received Get  Request | before Lock %+v, isLeader:%t, index: %d", kv.me, kv.stateMachine, isLeader, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
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
	kv.mu.Unlock()

	defer func() {
		// Clear the channel from the reponsewaiters map
		go kv.removeResponseWaiter(index)
	}()

	// Listen for an event of receving the applyMessage at expected index or for term change
	for {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case opResponse := <-responseWaiter:
			go func(timer *time.Timer) {
				if !timer.Stop() {
					<-timer.C
				}
			}(timer)

			// If the command that Raft applied, matches the command that this RPC handlder has submitted
			// (i.e RequestUUID and index matches)
			// the request has been sucessfully commited to stateMachine.
			// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
			if opResponse.Index == index && opResponse.RequestSeqID == args.RequestSeqID &&
				opResponse.ClientID == args.ClientID {
				reply.Err = OK
				kv.mu.Lock()
				reply.Value = kv.stateMachine[args.Key]
				kv.mu.Unlock()
			} else {
				reply.Value = ""
				reply.Err = ErrWrongLeader
			}
			return
		case <-timer.C:
			// Timer has fired and no message is received for the expected index in apply channel
			// Either KV server is partitioned, or Raft term has changed
			// Do nothing if term has not changed
			// If Raft term has progressed, reply error to the RPC request
			newTerm, _ := kv.rf.GetState()
			if newTerm > term {
				reply.Err = ErrWrongLeader
				reply.Value = ""
				return
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	response := kv.duplicateTable[args.ClientID]
	kv.mu.Unlock()

	Debug(dClient, "S%d received PutAppend | before lock, reponse: %+v, args: %+v", kv.me, response, args)

	reply.ServerID = kv.me
	if response.RequestSeqID == args.RequestSeqID && response.ClientID == args.ClientID {
		reply.Err = OK
		return
	}

	if args.RequestSeqID < response.RequestSeqID && response.ClientID == args.ClientID {
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

	index, term, isLeader := kv.rf.Start(command)
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

	defer func() {
		// Clear the channel from the reponsewaiters map
		go kv.removeResponseWaiter(index)
	}()

	// Listen for an event of receving the applyMessage at expected index or for term change
	for {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case opResponse := <-responseWaiter:
			go func(timer *time.Timer) {
				if !timer.Stop() {
					<-timer.C
				}
			}(timer)

			Debug(dClient, "S%d PutAppend | after received opResponse: %+v, args: %+v", kv.me, opResponse, args)
			// If the command that Raft applied, matches the command that this RPC handlder has submitted
			// (i.e RequestUUID and index matches)
			// the request has been sucessfully commited to stateMachine.
			// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
			if opResponse.Index == index && opResponse.RequestSeqID == args.RequestSeqID &&
				opResponse.ClientID == args.ClientID {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			return
		case <-timer.C:
			// Timer has fired and no message is received for the expected index in apply channel
			// Either KV server is partitioned, or Raft term has changed
			// Do nothing if term has not changed
			// If Raft term has progressed, reply error to the RPC request
			newTerm, _ := kv.rf.GetState()
			if newTerm > term {
				reply.Err = ErrWrongLeader
				return
			}
		}
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
	kv.persister = persister
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)

	kv.stateMachine = make(map[string]string)
	kv.opResponseWaiters = make(map[int]chan OpResponse)
	kv.duplicateTable = make(map[int64]OpResponse)

	kv.installStateFromSnapshot(persister.ReadSnapshot())

	return kv
}

// Processes incoming ApplyMsg values that were processed and passed from Raft.
// The main task of the method is to wake up the RPC handler that is waiting on applied message
// at particular log index
func (kv *KVServer) receiveApplyMessages() {
	for applyMsg := range kv.applyCh {
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

func (kv *KVServer) initKVServer() {
	for _, logEntry := range kv.rf.LogEntries() {
		op, ok := logEntry.Command.(Op)
		if ok {
			kv.duplicateTable[op.ClientID] = OpResponse{
				ClientID:     op.ClientID,
				RequestSeqID: op.RequestSeqID,
			}
			kv.applyOpToStateMachineL(op)
		}
	}
}

func (kv *KVServer) snapShotKVState(index int) {
	if kv.maxraftstate < 0 {
		return
	}

	if len(kv.persister.ReadRaftState()) >= 9*kv.maxraftstate/10 {
		data := kv.snapshotData()
		kv.rf.Snapshot(index, data)
	}
}

func (kv *KVServer) snapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.duplicateTable)
	return w.Bytes()
}

func (kv *KVServer) installStateFromSnapshot(data []byte) {
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
