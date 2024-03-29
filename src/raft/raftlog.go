package raft

// These methods implement methods to access and manipulate the Raft server's log items relative
// global, monotonically increasing log indexes (Compared to log indexing relative to state of log array, that might
// get truncated after installing snapshot).
type raftLog struct {
	Log             []LogEntry
	FirstEntryIndex int
}

// Returns the index of the last entry in the server's log.
// Takes into consideration index shifts in event of log truncation
// func (rf *Raft) lastLogIndexTerm() (int, int) {
// 	lastLogIndex := rf.log.lastLogIndex()
// 	lastLogTerm := rf.log[lastLogIndex].Term
// 	return lastLogIndex, lastLogTerm
// }

func (rl *raftLog) lastLogIndex() int {
	return rl.len() - 1
}

// Returns the index of the log entry relative to the server's current log array state
func (rl *raftLog) logArrayIndex(index int) int {
	return index - rl.FirstEntryIndex
}

// Returns the log entry at the global index value.
// Does not do any checks to verify if log is not empty or to verify index param correspond to
// to entry that is not in snapshot
func (rl *raftLog) entryAtIndex(index int) LogEntry {
	// calculate the index of element in log array.
	return rl.Log[rl.logArrayIndex(index)]
}

// Truncates all log entries starting with index provided in params
func (rl *raftLog) truncateLogSuffix(index int) {
	rl.Log = rl.Log[:rl.logArrayIndex(index)]
}

// Truncates all the log entries from beginning up to and including index
// Creates new log array to enable GC to reclaim memory of truncated items
// Resets the firstEntryIndex of the log to start on `index+1`
func (rl *raftLog) truncatePrefix(index int) {
	truncated := rl.Log[rl.logArrayIndex(index)+1:]
	newLog := make([]LogEntry, len(truncated))
	copy(newLog, truncated)
	rl.Log = newLog
	rl.FirstEntryIndex = index + 1
}

func (rl *raftLog) len() int {
	return len(rl.Log) + rl.FirstEntryIndex
}

func (rl *raftLog) logSuffix(index int) []LogEntry {
	return rl.Log[rl.logArrayIndex(index):]
}
