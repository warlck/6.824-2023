package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"

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
	Servers      map[int][]string
	GIDs         []int
	Shard        int
	GID          int
	Num          int
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
		Servers:      args.Servers,
		Op:           Join,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	sc.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	err, ret := sc.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	command := Op{
		GIDs:         args.GIDs,
		Op:           Leave,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	sc.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	err, ret := sc.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	command := Op{
		GID:          args.GID,
		Shard:        args.Shard,
		Op:           Move,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	sc.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	err, ret := sc.checkRepeatRequest(args.ClientID, args.RequestSeqID)
	if ret {
		reply.Err = err
		return
	}

	command := Op{
		Num:          args.Num,
		Op:           Query,
		RequestSeqID: args.RequestSeqID,
		ClientID:     args.ClientID,
	}

	sc.processRaftMessage(command, args.ClientID, args.RequestSeqID, reply)
	// Put the config data in Reply if Err is  OK
	if reply.WrongLeader {
		return
	}

	// Replies with the lastest configuration if f the number is -1 or bigger than the biggest known configuration number
	lastConfig := sc.configs[len(sc.configs)-1]
	if args.Num <= -1 || args.Num > lastConfig.Num {
		reply.Config = lastConfig
	} else {
		reply.Config = sc.configs[args.Num]
	}

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

				Debug(dCommit, "S%d received message: %+v, response: %+v ", sc.me, applyMsg, dupTableEntry)
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
		Debug(dCommit, "S%d applied message SM:%+v", sc.me, sc.configs)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyOpToStateMachineL(op Op) {
	var newConfig Config
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig = Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	switch op.Op {
	case Join:
		// get joinArg from Opp
		// process Join
		newConfig.addNewGroups(lastConfig.Groups, op.Servers)
		newConfig.reconfigShards()
		sc.configs = append(sc.configs, newConfig)
		Debug(dCommit, "S%d completed JOIN , newConfig:%+v, Servers: %+v", sc.me, newConfig, op.Servers)

	case Leave:
		// get leaveArg from Opp
		// process Leave
		newConfig.removeGIDs(lastConfig.Groups, op.GIDs)
		newConfig.reconfigShards()
		sc.configs = append(sc.configs, newConfig)
	case Move:
		copy(newConfig.Shards[:], lastConfig.Shards[:])
		newConfig.Shards[op.Shard] = op.GID
		newConfig.addNewGroups(lastConfig.Groups, nil)
		sc.configs = append(sc.configs, newConfig)
	}

}

func (c *Config) addNewGroups(oldGroups map[int][]string, newGroups map[int][]string) {
	for gid, servers := range oldGroups {
		c.Groups[gid] = servers
	}
	for gid, servers := range newGroups {
		c.Groups[gid] = servers
	}
}

func (c *Config) removeGIDs(oldGroups map[int][]string, gids []int) {
	gidsToRemove := make(map[int]bool)
	for _, gid := range gids {
		gidsToRemove[gid] = true
	}

	for gid, servers := range oldGroups {
		if !gidsToRemove[gid] {
			c.Groups[gid] = servers
		}
	}
}

func (c *Config) reconfigShards() {
	// sort GIDs,  config Shard to GIDS[ShardNumber % len(GIDs)]
	gids := []int{}
	for gid := range c.Groups {
		gids = append(gids, gid)
	}

	sort.Ints(gids)

	var gidN int
	for shardN := 0; shardN < NShards; shardN++ {
		if len(gids) > 0 {
			gidN = gids[shardN%len(gids)]
		} else {
			gidN = 0
		}

		c.Shards[shardN] = gidN
	}
}
