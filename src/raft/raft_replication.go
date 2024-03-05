package raft

import "time"

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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject log, get lower term [%d < %d] from %d", args.Term, rf.currentTerm, args.LeaderId)
		return
	}

	//
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[Term %d] to %d in Term %d", term, rf.me, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicationToPeer(peer, args)
	}

	return true
}
