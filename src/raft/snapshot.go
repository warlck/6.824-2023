package raft

// Type snapShot defines the data structure for storing Raft's snapshot and associated metadata such
// 'LastIncludedIndex' and 'LastIncludedTerm'
type snapShot struct {
	data              []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}
