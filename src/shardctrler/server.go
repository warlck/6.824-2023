package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Stores Channels that RPC hanlders are waiting on
	opResponseWaiters map[int]chan OpResponse

	// Duplicate table is used to prevent processing duplicate Put/Append/Get requests
	// sent by Clerk
	duplicateTable map[int64]OpResponse

	configs []Config // indexed by config num
}

type Op struct {
	Args         interface{}
	Op           string
	RequestSeqID int64
	ClientID     int64
}

type OpResponse struct {
	RequestSeqID int64
	Index        int
	ClientID     int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	err, ret := sc.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	command := Op{
		Args:         args,
		Op:           Join,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	err = sc.processRaftResponse(command, args.ClientID, args.RequestSeqID)
	reply.Err = err
	if err == OK {
		return
	} else if err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.opResponseWaiters = make(map[int]chan OpResponse)
	sc.duplicateTable = make(map[int64]OpResponse)

	go sc.receiveApplyMessages()

	return sc
}

// Clears the response waiter channel that was used to by RPC KVServer's RPC handlers
func (sc *ShardCtrler) removeResponseWaiter(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.opResponseWaiters, index)
}

func (sc *ShardCtrler) checkRepeatRequest(clientID, requestSeqID int64) (Err, bool) {
	sc.mu.Lock()
	response := sc.duplicateTable[clientID]
	sc.mu.Unlock()

	// Debug(dClient, "S%d received Join | before lock, reponse: %+v, args: %+v", kv.me, response, args)

	if response.RequestSeqID == requestSeqID && response.ClientID == clientID {
		return OK, true
	}

	if requestSeqID < response.RequestSeqID && response.ClientID == clientID {
		return ErrStaleRequest, true
	}
	return "", false
}

func (sc *ShardCtrler) processRaftResponse(command Op, clientID int64, requestSeqID int64) Err {
	index, term, isLeader := sc.rf.Start(command)
	//Debug(dClient, "S%d received Get  Request | before Lock %+v, isLeader:%t, index: %d", kv.me, kv.stateMachine, isLeader, index)
	if !isLeader {
		return ErrWrongLeader
	}

	// Debug(dClient, "S%d received Get  Request | before lock, args: %+v, index: %d, isLeader: %t",
	// kv.me, args, index, isLeader)
	sc.mu.Lock()
	// Debug(dClient, "S%d received Get  Request | after lock", kv.me)

	// We need to check if ApplyMsg receiver has  already received an apply message with current index
	// If the apply message with current index already has been received, responseWaiter will contain a buffered channel with
	// buffer of 1. We can read the value in the buffered channel and return to client
	responseWaiter, ok := sc.opResponseWaiters[index]
	// If apply message with current index has not been processsed yet, create a channel that we use to wait
	// for  Raft processing to complete.
	if !ok {
		responseWaiter = make(chan OpResponse, 1)
		sc.opResponseWaiters[index] = responseWaiter
	}
	sc.mu.Unlock()

	defer func() {
		// Clear the channel from the reponsewaiters map
		go sc.removeResponseWaiter(index)
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
			// (i.e RequestID and index matches)
			// the request has been sucessfully commited to stateMachine.
			// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
			if opResponse.Index == index && opResponse.RequestSeqID == requestSeqID &&
				opResponse.ClientID == clientID {
				return OK
			} else {
				return ErrWrongLeader
			}
		case <-timer.C:
			// Timer has fired and no message is received for the expected index in apply channel
			// Either SC server is partitioned, or Raft term has changed
			// Do nothing if term has not changed
			// If Raft term has progressed, reply error to the RPC request
			newTerm, _ := sc.rf.GetState()
			if newTerm > term {
				return ErrWrongLeader
			}
		}
	}
}

// Processes incoming ApplyMsg values that were processed and passed from Raft.
// The main task of the method is to wake up the RPC handler that is waiting on applied message
// at particular log index
func (sc *ShardCtrler) receiveApplyMessages() {
	for {
		if sc.killed() {
			return
		}

		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if ok {
				dupTableEntry := sc.duplicateTable[op.ClientID]
				response := OpResponse{
					RequestSeqID: op.RequestSeqID,
					ClientID:     op.ClientID,
					Index:        applyMsg.CommandIndex,
				}

				// Debug(dCommit, "S%d received message: %+v, response: %+v ", kv.me, applyMsg, dupTableEntry)
				if op.RequestSeqID > dupTableEntry.RequestSeqID {
					sc.duplicateTable[op.ClientID] = response
					sc.applyOpToStateMachineL(op)
				}

				reponseWaiter, ok := sc.opResponseWaiters[applyMsg.CommandIndex]
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
					sc.opResponseWaiters[applyMsg.CommandIndex] = bufferedWaiter
				}

			}
		}
		// Debug(dCommit, "S%d applied message SM:%+v", kv.me, kv.stateMachine)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyOpToStateMachineL(op Op) {
	switch op.Op {
	case Join:

	}
}
