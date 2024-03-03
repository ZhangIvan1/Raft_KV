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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

type Role string

const (
	Follower  Role = "Follower"
	Leader    Role = "Leader"
	Candidate Role = "Candidate"
)

// Upper and lower bounds for election timeout
const (
	electionTimeoutMin   time.Duration = 250 * time.Millisecond
	electionTimeoutMax   time.Duration = 400 * time.Millisecond
	electionTimeoutRange int64         = int64(electionTimeoutMax - electionTimeoutMin)
)

// Reset the election timer when receiving the leader's log or voting.
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%electionTimeoutRange) // Random timeout
}

// Check whether the current timeout
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	voteFor     int
	role        Role

	electionStart   time.Time     // when the election starts
	electionTimeout time.Duration // for random election time
}

// state transition
func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "Candidate %d has become a Leader", rf.me)
	rf.role = Leader
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Only Follower can become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s become a Candidate for Term %d", rf.me, rf.currentTerm+1)
	rf.currentTerm += 1
	rf.role = Candidate
	rf.voteFor = rf.me
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Still in Term %d, get lower Term %d", rf.currentTerm, term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s become a Follower, from Term %d to Term %d", rf.me, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.voteFor = -1
	}
	rf.currentTerm = term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//return term, isleader
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
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

	// If the request term is smaller than current -> rejected
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "%s reject vote, still in Term %d, get lower Term %d", rf.me, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		return
	}

	// If the requested term is larger than current -> accept
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// If the peer has already voted
	if rf.voteFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "Vote Request failed to %d, %d has already voted to %d", args.CandidateId, rf.me, rf.voteFor)
		reply.VoteGranted = false
		return
	}

	// Return to voting commitment
	reply.VoteGranted = true      // Said would not initiate new elections for the time being
	rf.voteFor = args.CandidateId //
	rf.resetElectionTimerLocked() // Reset election timer
	LOG(rf.me, rf.currentTerm, DVote, "Raft %s vote to Raft %s in Term %d", rf.me, args.CandidateId, args.Term)
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

	// Your code here (PartB).

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

// The logic of elections

func (rf *Raft) letElection(term int) bool {
	gotVotes := 0

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

		// peer votes for self
		if reply.VoteGranted {
			gotVotes++
		}

		// When get more than half of the votes
		if gotVotes > len(rf.peers)>>2 {
			rf.becomeLeaderLocked()
			// Initiate heartbeat and log replication
			go rf.replicationTicker(term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Lock()

	// Terminate the election if the current role and term change
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply in Term %d", rf.currentTerm)
		return false
	}

	// Traverse all peers to request votes
	for peer := 0; peer < len(rf.peers); peer++ {
		// If self, vote self
		if peer == rf.me {
			gotVotes++
			continue
		}

		// Set parameters for requesting vote
		args := &RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		go askForVote(peer, args)
	}

	return true
}

// Determine whether the current role and term have changed
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			// When a not-leader and the election timer times out, enters the election state.
			rf.becomeCandidateLocked()
			go rf.letElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (PartA, PartB, PartC).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
