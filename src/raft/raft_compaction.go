package raft

import (
	"fmt"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DSnap, "snap on %d", index)

	// if the gaven index or haven't commit
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}

	// if the gaven index already inside snapshot
	if index <= rf.log.snapLastIndex {
		LOG(rf.me, rf.commitIndex, DSnap, "Already snapshot in [%d, %d]", rf.log.snapLastIndex+1, rf.commitIndex)
		return
	}

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

//
type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

//
func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

//
type InstallSnapshotReply struct {
	Term int
}

//
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "receive snapshot from %d, Args=%v", args.LeaderId, args.String())

	// initialized the reply
	reply.Term = rf.currentTerm
	// check term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "reject snapshot from %d, get a lower term T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term >= rf.currentTerm { // follower or candidate
		rf.becomeFollowerLocked(args.Term)
	}

	// check if it is a repeated snapshot
	if rf.log.snapLastIndex >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "reject snapshot from %d, already include this snap S%d>S%d", args.LeaderId, rf.log.snapLastIndex, args.LastIncludedIndex)
		return
	}

	// receive the snapshot (install to memory/persist/application)
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true // tell applier to apply the snapshot
	rf.applyCond.Signal()
}

//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "send install to %, lost or crashed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "send install to %d, install snapshot, reply=%v", peer, reply.String())

	//
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	//
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "")
		return
	}

	// update follower's match index and term
	if args.LastIncludedIndex > rf.matchIndex[peer] { // keep matchIndex increase
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}

	// There is no need to update the commitIndex, because all snapshots must be committed after installation.
}
