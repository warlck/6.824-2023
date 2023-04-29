package raft

type snapshot struct {
	Data              []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}
