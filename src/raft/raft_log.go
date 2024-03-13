package raft

import (
	"course/labgob"
	"fmt"
)

// RaftLog mixes snapshotted logs and normal logs
type RaftLog struct {
	snapLastIndex int // the index of the last log in the snapshot
	snapLastTerm  int // the term of the last log in the snapshot

	// Log snapshot (the first part of the hole log)
	snapshot []byte // [1, snapLastIndex]
	// the rest of the log (not snapshotted)
	tailLog []LogEntry // (snapLastIndex, snapLastIndex+len(tailLog)-1]
}

// NewLog make a new RaftLog
func NewLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}

	// add a fake log at the beginning of the tailLog, so that can get the prevLog normally which inside the snapshot
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	// appending the real logs
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// Persistors for RaftLog
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIndex int
	if err := d.Decode(&lastIndex); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIndex = lastIndex

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term faild")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode last include log failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persistLocked(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// return the size of hole logs
func (rl *RaftLog) size() int { return rl.snapLastIndex + len(rl.tailLog) }

// index covert
func (rl *RaftLog) logicalIndexToPhysical(logicalIndex int) int {
	// if logical index fall beyond [snapLastIndex, size()-1]
	if logicalIndex < rl.snapLastIndex || logicalIndex > rl.size()-1 {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicalIndex, rl.snapLastIndex, rl.size()-1))
	}
	return logicalIndex - rl.snapLastIndex
}

// get the real log at logicalIndex
func (rl *RaftLog) at(logicalIndex int) LogEntry {
	return rl.tailLog[rl.logicalIndexToPhysical(logicalIndex)]
}

//get last log and its term
func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIndex + i, rl.tailLog[i].Term
}

// to find out the first log in a term
func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	// no log in the term in this server
	return InvalidIndex
}

// get the tail after the gaven index
func (rl *RaftLog) tail(logicalIndex int) []LogEntry {
	if logicalIndex >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.logicalIndexToPhysical(logicalIndex):]
}

// append after the ending
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

// append after the index
func (rl *RaftLog) appendFrom(LogicalPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.logicalIndexToPhysical(LogicalPrevIndex)+1], entries...)
}

// doing snapshot which is from the application layer
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	if index <= rl.snapLastIndex {
		return
	}

	// located the true index of the log in tailLog
	idx := rl.logicalIndexToPhysical(index)

	// update raftLog
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	// make a new log array to release old memory space
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIndex)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// installing snapshot which is from the leader
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	// update raftLog
	rl.snapLastTerm = term
	rl.snapLastIndex = index
	rl.snapshot = snapshot

	// make a new log array to release old memory space
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}

// covert logs to string
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIndex
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIndex+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIndex+len(rl.tailLog)-1, prevTerm)
	return terms
}
