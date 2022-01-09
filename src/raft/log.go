package raft

import (
	"log"
	"math"
)

const noLimit = math.MaxInt32

type raftLog struct {
	//snapshot *Snapshot
	//offset  int

	logs         []LogEntry
	lastLogIndex int
	commitIndex  int
	lastApplied  int
	applyCh      chan ApplyMsg
}

func (l *raftLog) commitTo(i int) {
	if l.commitIndex < i {
		if l.lastIndex() < i {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", i, l.lastIndex())
		}
		l.commitIndex = i
	}
}

func (l *raftLog) appliedTo(i int) {
	if i == 0 {
		return
	}
	if l.commitIndex < i || l.lastLogIndex < i {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.lastApplied, l.commitIndex)
	}
	// TODO apply command
	//entry := l.entries(i, 1)[0]
	//l.applyCh <- ApplyMsg{Command: entry.Command, CommandIndex: entry.Idx}
	l.lastApplied = i
}

func (l *raftLog) lastTerm() int {
	return l.term(l.lastLogIndex)
}

func (l *raftLog) term(i int) int {
	term := 0
	if i > 0 {
		term = l.logs[i-1].Term
	}
	return term
}

func (l *raftLog) lastIndex() int {
	return l.lastLogIndex
}

func (l *raftLog) append(entry ...LogEntry) {
	l.logs = append(l.logs, entry...)
	l.lastLogIndex += len(entry)
}

func (l *raftLog) entries(start int, maxsize int) []LogEntry {
	return l.slice(start, l.lastIndex(), maxsize)
}

func (l *raftLog) cutDown(lastIndex int) {
	l.entries(1, noLimit)
	l.lastLogIndex = lastIndex
}

// [start...end]
func (l *raftLog) slice(startIndex int, endIndex int, maxsize int) []LogEntry {
	if startIndex > l.lastIndex() {
		return nil
	}

	entries := l.logs[startIndex-1 : endIndex]
	if maxsize == noLimit || maxsize > l.lastIndex() {
		return entries
	} else {
		return entries[:maxsize]
	}
}

func (l *raftLog) maybeCommit(maxIndex int, term int) bool {
	if maxIndex > l.commitIndex && l.term(maxIndex) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}
