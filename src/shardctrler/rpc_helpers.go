package shardctrler

import "time"

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

	if response.RequestSeqID == requestSeqID && response.ClientID == clientID {
		return OK, true
	}

	if requestSeqID < response.RequestSeqID && response.ClientID == clientID {
		return ErrStaleRequest, true
	}
	return "", false
}

func (sc *ShardCtrler) processRaftMessage(command Op, clientID int64, requestSeqID int64, r Replier) {
	index, term, isLeader := sc.rf.Start(command)

	if !isLeader {
		r.ReplyWrongLeader()
		return
	}

	sc.mu.Lock()

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
				r.ReplyOK()
				return
			} else {
				r.ReplyWrongLeader()
				return
			}
		case <-timer.C:
			// Timer has fired and no message is received for the expected index in apply channel
			// Either SC server is partitioned, or Raft term has changed
			// Do nothing if term has not changed
			// If Raft term has progressed, reply error to the RPC request
			newTerm, _ := sc.rf.GetState()
			if newTerm > term {
				r.ReplyWrongLeader()
				return
			}
		}
	}
}
