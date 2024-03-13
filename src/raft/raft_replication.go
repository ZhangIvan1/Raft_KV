package raft

import (
	"fmt"
	"sort"
	"time"
)

// replicationTicker
// only runnable in given term
// |-> if no-log = heartbeat
// |-> if has-log = log replication
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {

		ok := rf.startReplication(term)
		if !ok {
			break
		}

		// pause 200 ms
		time.Sleep(replicateInterval)
	}
}

type LogEntry struct {
	Term         int         // LogEntry's term
	CommandValid bool        // if this should be applied?
	Command      interface{} // the command should be applied to the state machine
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients

	PrevLogIndex int        // index of log entry immediately preceding
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; mat send more than one for efficiency)

	LeaderCommit int // leader's commitIndex witch to update the follower's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	ConfilictIndex int
	ConfilictTerm  int
}

// convert AppendEntriesArgs to String
func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "Receive log from %d, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	// initialized reply
	reply.Term = rf.currentTerm
	reply.Success = false

	// Align term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject log, get lower term [%d < %d] from %d", args.Term, rf.currentTerm, args.LeaderId)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// after receive an acceptable we should reset election timer for respect to leader
	defer rf.resetElectionTimerLocked()

	// the log should match in same <index, term>
	// if prevLog not matched
	if args.PrevLogIndex >= rf.log.size() {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject log from %d, Follower %d log too short, Len:%d<=Perv:%d", args.LeaderId, rf.me, rf.log.size(), args.PrevLogIndex)
		// return the follower log index and term
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidTerm
		return
	}
	// if log in difference term
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject log from %d, Prev log not match, [%d]:T%d!=[%d]:T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex), args.PrevLogIndex)
		// return the first log in leader term if follower has
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		return
	}

	// log matched, accept
	// insert new logs after the matching point and throw unmatched logs
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog2, "Follower %d accept logs: (%d, %d]", rf.me, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	//handle the args.LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal() // call application go on
	}

	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// find the median of the progress of a log match
func (rf *Raft) getMajorityIndexLocked() int {
	index := make([]int, len(rf.peers))
	copy(index, rf.matchIndex)
	sort.Ints(index)
	majorityIndex := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DApply, "Match index after sort: %v, majority[%d]=%d", index, majorityIndex, index[majorityIndex])
	return index[majorityIndex]

}

// only valid in the given term
func (rf *Raft) startReplication(term int) bool {

	replicationToPeer := func(peer int, args *AppendEntriesArgs) {

		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		// handle the response
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "send Replication to %d failed", peer)
			return
		}

		// If response term is greater, become followers
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "Replication to peer%d, Context lost, T%d:Leader -> T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// handle the reply
		// mismatch, append failed
		if !reply.Success {

			//// go back an index
			//idx := rf.nextIndex[peer] - 1
			//term := rf.log[idx].Term
			//// search for the match log of the peer
			//for idx > 0 && rf.log[idx].Term == term {
			//	idx--
			//}
			//rf.nextIndex[peer] = idx + 1
			//LOG(rf.me, rf.currentTerm, DLog, "Lost match in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex)

			prevIndex := rf.nextIndex[peer]
			// if follower do not have leader's term (too short)
			if reply.ConfilictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				// find if leader have follower's term
				firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
				//if do have
				if firstTermIndex != InvalidIndex {
					// match to the first log of term
					rf.nextIndex[peer] = firstTermIndex + 1
				} else { // if not have
					// jump forward of follower's (the ending of the last term +1)
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// keep nextIndex reducing
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			return
		}

		// matched and appended
		// update match/next index
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// compute the new commitIndex
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal() // call application go on
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[Term %d] to %d in Term %d", term, rf.me, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		// for leader-self
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1 // log matching point of the peer and leader
			rf.nextIndex[peer] = rf.log.size()      // the next log that the peer should receive from the leader
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		// if target log has been persisted
		if prevIdx < rf.log.snapLastIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIndex,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "to s%d, Send Snapshot , Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}
		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			//Entries:      rf.log[prevIdx+1:],
			Entries:      append([]LogEntry(nil), rf.log.tail(prevIdx+1)...),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "to s%d, Append , Args=%v", peer, args.String())
		go replicationToPeer(peer, args)
	}

	return true
}
