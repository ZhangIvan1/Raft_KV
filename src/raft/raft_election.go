package raft

import (
	"math/rand"
	"time"
)

// Reset the election timer when receiving the leader's log or voting.
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange) // Random timeout
}

// Check whether the current timeout
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// check if whether local log is newer than the log from the candidate
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	// get local index and term
	//logLen := rf.log.size()
	lastIndex, lastTerm := rf.log.last()

	LOG(rf.me, rf.currentTerm, DVote, "compare last log, local[%d]T%d, candidate[%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	// Term matter
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	// then index
	return lastIndex > candidateIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int // candidate's term
	CandidateId int // candidate requesting vote

	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Construct return value
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If the request term is smaller than current -> rejected
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "%d reject vote, still in Term %d, get lower Term %d", rf.me, rf.currentTerm, args.Term)
		return
	}

	// If the requested term is larger than current -> accept
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// If the peer has already voted
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "Vote Request failed to %d, %d has already voted to %d", args.CandidateId, rf.me, rf.voteFor)
		return
	}

	// check who has the latest log
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "%d reject vote, candidate has old log", rf.me)
		return
	}

	// Return to voting commitment
	reply.VoteGranted = true      // Said would not initiate new elections for the time being
	rf.voteFor = args.CandidateId //
	rf.persistLocked()
	rf.resetElectionTimerLocked() // Reset election timer
	LOG(rf.me, rf.currentTerm, DVote, "Raft %d vote to Raft %d in Term %d", rf.me, args.CandidateId, args.Term)
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

// The logic of elections
func (rf *Raft) startElection(term int) {
	votes := 0

	// RPC
	askForVote := func(peer int, args *RequestVoteArgs) {

		// Return value structure
		reply := &RequestVoteReply{}
		// Send RPC
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the response
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote to %d failed", peer)
			return
		}

		// If response term is greater, become followers
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// Check if the context has changed
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply in Term %d", rf.currentTerm)
			return
		}

		// count votes
		if reply.VoteGranted {
			votes++
			LOG(rf.me, rf.currentTerm, DVote, "%d got %d votes in Term %d", rf.me, votes, rf.currentTerm)
			// When get more than half of the votes
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				// Initiate heartbeat and log replication
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Terminate the election if the current role and term change
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply in Term %d", rf.currentTerm)
		return
	}

	// Traverse all peers to request votes
	//logLen := len(rf.log)
	lastIndex, lastTerm := rf.log.last()
	for peer := 0; peer < len(rf.peers); peer++ {
		// If self, vote self
		if peer == rf.me {
			votes++
			continue
		}

		// Set parameters for requesting vote
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,

			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		go askForVote(peer, args)
	}

	return
}

//
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			// When a not-leader and the election timer times out, enters the election state.
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
