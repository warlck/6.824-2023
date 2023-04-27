package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type state int

const (
	follower  state = 1
	leader    state = 2
	candidate state = 3

	hearbeatTimeout = time.Millisecond * 100
	// Raft Paper Section 5.6 describes electionTimeout to be ~ 20-25x of broadcastTime.
	// Due to characteristic of the testing system: Max 10 heartbeats per second and
	// Leader election is required to happen within 5 seconds ;
	// the heartbeatTimeout and electionTimeout parameters are chosen to suit the testing setup.

)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry - defines a data structure that is being stored in Raft Server's log.
// Each LogEntry contains a command for state machine, and term when entry was received by leader
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // CandidateId that received vote in current term (or -1 if none)

	// Current state at which Raft Server is operation. Can be Follower, Leader, Candidate
	currentState state

	// timerReset - Stores the current time whenever valid RPC from a leader or candidate.
	// Used by leader election function to check if current Raft server needs to start new election
	electionTimerReset time.Time

	electionTimeout time.Duration
	// Raft Server's log of logEntries.
	// Each log entry has a command for state machine that is received from client and term when entry was received
	// by leader (first index is 1)
	log []LogEntry

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increase	monotonically)
	lastApplied int

	// *** Volatile State ***
	// Need to be reinitialized after election
	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	// Channel used to synchronize the applying of new entries when commitIndex is updated
	commitCh chan int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.currentState == leader)
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logEntries []LogEntry
	var votedFor int
	var currentTerm int
	var err error

	// ***********************
	// Order of decoding is important. Shall follow same order as
	// persist function above
	// ***********************
	if err = d.Decode(&logEntries); err != nil {
		log.Fatal("Failed to read log from persistent state", err)
	}

	if err = d.Decode(&votedFor); err != nil {
		log.Fatal("Failed to read votedFor from persistent state", err)
	}

	if err = d.Decode(&currentTerm); err != nil {
		log.Fatal("Failed to read currentTerm from persistent state", err)
	}

	rf.log = logEntries
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	// Debug(dVote, "S%d received Request Vote RPC args: %+v, currentTerm: %d, lastLogIndex: %d, lasLogTerm:%d", rf.me, args, currentTerm, lastLogIndex, lastLogTerm)

	reply.Term = currentTerm
	if currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	if currentTerm < args.Term {
		rf.revertToFollowerState(args.Term)
	}

	candidateIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		candidateIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		candidateIsUpToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && candidateIsUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.electionTimerReset = time.Now()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	// Log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries      []LogEntry
	LeaderCommit int // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // Term number of the conflicting entry
	XIndex  int  // Index of the first log entry with conflicting term
	XLen    int  // Length  of the follower's log
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	currentState := rf.currentState
	lastLogIndex, _ := rf.lastLogIndexTerm()

	reply.Term = currentTerm
	reply.Success = false
	reply.XIndex = -1
	reply.XTerm = -1
	reply.XLen = -1

	Debug(dInfo, "S%d, received AppendEntries RPC from S%d, args = %+v", rf.me, args.LeaderId, args)

	if currentTerm > args.Term {
		return
	}

	if currentTerm < args.Term || currentState == candidate {
		rf.revertToFollowerState(args.Term)
	}

	// Received valid AppendEntries from current leader, can reset the electiom timer
	rf.electionTimerReset = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if lastLogIndex < args.PrevLogIndex {
		reply.XLen = len(rf.log)
		return
	}

	prevLogIndexEntry := rf.log[args.PrevLogIndex]
	if prevLogIndexEntry.Term != args.PrevLogTerm {
		reply.XTerm = prevLogIndexEntry.Term
		reply.XIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	// Compare log entries of the follower and log entries in the arg.Entries to find the index of first conflicting entry
	// (if any) int the follower log and in the args.Entries.
	var i, j int
	for i, j = args.PrevLogIndex+1, 0; i <= lastLogIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			// Found conflicting entry, prune all following
			// log entries starting with conflicting one.
			rf.log = rf.log[:i]
			break
		}

	}
	// Debug(dInfo, "S%d has run loop foundConflict: %t, i:%d   j:%d,  lastLogIndex: %d, len(args.Entries): %d", rf.me, foundConflict, i, j, lastLogIndex, len(args.Entries))

	// Delete the conflicting entry and all the follow it from the follower's log
	// Appends any new entries not already in the log
	newEntries := args.Entries[j:]
	rf.log = append(rf.log, newEntries...)
	if len(newEntries) > 0 {
		rf.persist()
	}

	reply.Success = true

	Debug(dInfo, "S%d replying succes to AppendEntries RPC from S%d, args = %+v", rf.me, args.LeaderId, args)

	if args.LeaderCommit > rf.commitIndex {
		min := min(args.LeaderCommit, len(rf.log)-1)
		rf.commitIndex = min
		go rf.UpdatedCommitIndex(min)
	}
}

// startArgreement -This method takes index of the latest log entry that we need to agree on and starts
// agreement process on that new entry for each follolwer.
// The method also passes the term of the latest log entry down to sendAppentries method as consistency check.
// If during the process of agreeing on the element, the term of leader Raft server changes, we shall stop the
// agreement  and return immediately.
//
// Following the Rules for Leaders section of Figure 2. of Raft paper, this method needs to correctly
// identify which elements to send for each server, based on the according nextIndex[i] value.
// If nextIndex[i] is smaller then or equal to newLogIndex, gets log entries starting at newLogIndex and ending with
// newLogIndex and resets the newLogIndex to be nextIndex[i]. Calls AppendEntries RPC with updated values for the follower.
// Otherwise, sends the element at newLogIndex.
func (rf *Raft) startAgreement(newLogIndex int, newLogTerm int) {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i)
	}

}

// sendAppendEntries - Makes AppendEntries RPC to follower identified by `server` parameter.
// This methods assumes that correct log entries (i.e entries that leader thinks are not yet copied to
// follower) are passed as parameter, together with correct  index of first log entry that leader
// believes to be in disagreement with the the follower's log.
//
// The `term` parameter is used for conistency check that ensures leader's term does not change
// in the case of old RPC replies. If the term changes from the moment when we start agreeing on
// log entries to moment when we receive reply to AppendEntries RPC, we stop the agreement and return.
//
// This method implements leader's functionality for agreement on log entries according to
// specification defined in Rules for Leader's section of Figure 2. in Raft paper:
// *******************************************************************************
// If reply to AppendEntries RPC is successful: update nextIndex and matchIndex for follower -
// Even though parallel executions of `sendAppendEntries` method can be happening with different
// logEntries and newLogIndex values, the current execution can only make decision based
// on the entries and newLogIndex that is passed as params to the method.
// Hence we update as `matchIndex[i] = prevLogIndex + len(entries)`, where prevLogIndex
// and entries are values that were passed to AppendEntries RPC call with successful reply.
// *******************************************************************************
// If AppendEntries fails (reply is not success) because of log inconsistency: decrement nextIndex and retry
// In normal operation (without backtracking optimization), we shall set nextIndex[i] to prevLogIndex of failed
// AppendEntries call and decrement prevLogIndex accordingly.
func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	// Debug(dLeader, "S%d log of the leader:  %v", rf.me, rf.log)

	if rf.currentState != leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex >= len(rf.log) {
		prevLogIndex = len(rf.log) - 1
	}
	prevLogTerm := rf.log[prevLogIndex].Term

	entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	if len(args.Entries) > 0 {
		Debug(dLeader, "S%d sending  AppenEntried to S%d, with args = %+v", rf.me, server, args)
	}
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if ok {
		rf.processAppendEntriesReply(server, &args, &reply)
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	isLeader = rf.currentState == leader

	if isLeader {
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.persist()
		index = len(rf.log) - 1
		go rf.startAgreement(index, term)
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var resetTime time.Time
	var currentState state
	var electionTimeout time.Duration

	for !rf.killed() {

		// Debug(dTimer, "S%d,  sleeping random millseconds = %d", rf.me, time.Duration(randSleepTimer).Milliseconds())

		rf.mu.Lock()
		resetTime = rf.electionTimerReset
		currentState = rf.currentState
		electionTimeout = rf.electionTimeout
		rf.mu.Unlock()

		if currentState != leader && time.Since(resetTime) > electionTimeout {
			// Debug(dTimer, "S%d,  election timeout elapsed, starting election, currentState = %d", rf.me, currentState)
			go rf.startElection()
		}

		if currentState == leader {
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(server int) {
						rf.sendAppendEntries(server)
					}(i)
				}
				// Debug(dLeader, "S%d is sending heartbeat to S%d, lastLogIndex = %d", rf.me, i, lastLogIndex)
			}
		}
		time.Sleep(hearbeatTimeout)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.votedFor = -1
	rf.currentState = follower
	rf.electionTimeout = newElectionTimeout()

	// Initilize Raft Server log with sigle, empty logEntry. This ensures that  index of the first
	// logEntry with client commands starts at 1.
	rf.log = []LogEntry{{}}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitCh = make(chan int)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMessages()

	return rf
}

type electionState struct {
	votes    int
	finished bool
	mu       sync.Mutex
}

func (rf *Raft) requestVote(peer int, args RequestVoteArgs, elections *electionState) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.revertToFollowerState(reply.Term)
			return
		}

		if reply.VoteGranted {
			elections.mu.Lock()
			elections.votes += 1
			votes := elections.votes
			elections.mu.Unlock()
			if votes > len(rf.peers)/2 {
				if rf.currentTerm == args.Term && rf.currentState == candidate {
					rf.BecomeLeaderL(args.Term)
				}
			}
		}
	}

}

func (rf *Raft) requestVotesL() {
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	elections := electionState{votes: 1}

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, requestVoteArgs, &elections)
		}
	}
}

// startElection -
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentState = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimerReset = time.Now()
	rf.electionTimeout = newElectionTimeout()
	rf.persist()

	rf.requestVotesL()

	// Debug(dLog2, "S%d is starting an election ; candidateTerm = %d ", rf.me, rf.currentTerm)

}

// BecomeLeader -
func (rf *Raft) BecomeLeaderL(candidateTerm int) {
	rf.currentState = leader

	// Reinitiatilize volatile state of leaders
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	//go rf.sendHeartBeats(candidateTerm)
}

func (rf *Raft) sendHeartBeats(candidateTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		currentTerm := rf.currentTerm

		rf.mu.Unlock()

		if currentTerm > candidateTerm {
			return
		}

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			// Debug(dLeader, "S%d is sending heartbeat to S%d, lastLogIndex = %d", rf.me, i, lastLogIndex)

			go func(server int) {
				rf.sendAppendEntries(server)
			}(i)
		}

		time.Sleep(hearbeatTimeout)
	}

}

func (rf *Raft) UpdatedCommitIndex(newCommitIndex int) {
	rf.commitCh <- newCommitIndex
}

func (rf *Raft) applyMessages() {
	for newCommitIndex := range rf.commitCh {
		entiesToApply := make([]ApplyMsg, 0)
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		if newCommitIndex > lastApplied {
			for i := lastApplied + 1; i <= newCommitIndex; i++ {
				entiesToApply = append(entiesToApply, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				})
			}
			rf.lastApplied = newCommitIndex
			rf.mu.Unlock()

			for _, entry := range entiesToApply {
				rf.applyCh <- entry
			}
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) processAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	Debug(dLeader, "S%d received reply  from S%d reply = %+v with args = %+v", rf.me, server, reply, args)

	if reply.Term > currentTerm {
		rf.revertToFollowerState(reply.Term)
		return false
	}

	//If successful: update nextIndex and matchIndex for follower
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.updatedMatchIndex(rf.matchIndex[server])
		return false
	} else
	//  AppendEntries fails because of log inconsistency: decrement nextIndex and retry
	{

		newNextIndex := max(1, rf.nextIndex[server]-1)
		if reply.XLen > 0 {
			newNextIndex = reply.XLen
		} else if reply.XTerm > 0 {
			var i int
			foundXTerm := false

			for i = args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					foundXTerm = true
				}

				if rf.log[i].Term < reply.XTerm {
					break
				}
			}

			if !foundXTerm {
				newNextIndex = reply.XIndex
			} else {
				newNextIndex = i + 1
			}
		}
		rf.nextIndex[server] = newNextIndex
	}
	return true
}

// *** Methods that assume the lock is aquired. ****
func (rf *Raft) lastLogIndexTerm() (int, int) {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) revertToFollowerState(newTerm int) {
	if rf.currentTerm <= newTerm {
		rf.currentState = follower
		rf.currentTerm = newTerm
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) updatedMatchIndex(newMatchIndex int) {
	counter := 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] >= newMatchIndex {
			counter++
		}
	}

	if counter > len(rf.peers)/2 && rf.log[newMatchIndex].Term == rf.currentTerm {
		rf.commitIndex = newMatchIndex
		go rf.UpdatedCommitIndex(newMatchIndex)
	}
}
