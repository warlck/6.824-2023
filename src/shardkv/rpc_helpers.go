package shardkv

import "time"

// Clears the response waiter channel that was used to by RPC KVServer's RPC handlers
func (kv *ShardKV) removeResponseWaiter(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.opResponseWaiters, index)
}

func (kv *ShardKV) checkRepeatRequest(clientID, requestSeqID int64) (Err, bool) {
	kv.mu.Lock()
	response := kv.duplicateTable[clientID]
	kv.mu.Unlock()

	// Debug(dClient, "S%d received Join | before lock, reponse: %+v, args: %+v", kv.me, response, args)

	if response.RequestSeqID == requestSeqID && response.ClientID == clientID {
		return OK, true
	}

	if requestSeqID < response.RequestSeqID && response.ClientID == clientID {
		return ErrStaleRequest, true
	}
	return "", false
}

func (kv *ShardKV) processRaftMessage(command Op, clientID int64, requestSeqID int64, r Replier) {
	index, term, isLeader := kv.rf.Start(command)
	//Debug(dClient, "S%d received Get  Request | before Lock %+v, isLeader:%t, index: %d", kv.me, kv.stateMachine, isLeader, index)
	if !isLeader {
		r.ReplyWrongLeader()
		return
	}

	kv.mu.Lock()

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

			Debug(dLeader, "S%d-%d Raft message handler | after received opResponse: %+v, index: %d, requestSeqID: %d, clientID: %d", kv.me, kv.gid, opResponse, index, requestSeqID, clientID)
			// If the command that Raft applied, matches the command that this RPC handlder has submitted
			// (i.e RequestID and index matches)
			// the request has been sucessfully commited to stateMachine.
			// Othwerwise, Raft server have lost the leadership and another leader submitted different entry to stateMachine.
			if opResponse.Index == index && opResponse.RequestSeqID == requestSeqID &&
				opResponse.ClientID == clientID {
				r.ReplyOK()
				return
			} else {
				r.ReplyWrongLeader()
				return
			}
		case <-timer.C:
			// Timer has fired and no message is received for the expected index in apply channel
			// Either kv server is partitioned, or Raft term has changed
			// Do nothing if term has not changed
			// If Raft term has progressed, reply error to the RPC request
			newTerm, _ := kv.rf.GetState()
			if newTerm > term {
				r.ReplyWrongLeader()
				return
			}
		}
	}
}

func (kv *ShardKV) keyIsServedByGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(key)
	gid := kv.scs.currentConfig.Shards[shard]
	configNum := kv.scs.currentConfig.Num
	Debug(dInfo, "S%d-%d received RPC request , checking if keyIsServedByGroup, key:%s,   kv.gid: %+v, gid:%d,  shard: %d, kv.shardConfig: %+v, kv.shardIsReadyToServe[shard]: %+v", kv.me, kv.gid, key, kv.gid, gid, shard, kv.scs.currentConfig, kv.scs.shardIsReadyToServe[configNum][shard])
	return gid == kv.gid && kv.scs.shardIsReadyToServe[configNum][shard]
}
