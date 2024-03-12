package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	// Example:
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.xxx)
	//e.Encode(rf.yyy)
	//raftstate := w.Bytes()
	//rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// persistent state on all servers
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	//e.Encode(rf.log)
	rf.log.persistLocked(e)

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %s", rf.stateToString())
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error!!")
		return
	}
	rf.currentTerm = currentTerm

	var voteFor int
	if err := d.Decode(&voteFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read voteFor error!!")
		return
	}
	rf.voteFor = voteFor

	//var log []LogEntry
	//if err := d.Decode(&log); err != nil {
	//	LOG(rf.me, rf.currentTerm, DPersist, "Read log error!!")
	//	return
	//}
	//rf.log = log
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error!!")
		return
	}

	LOG(rf.me, rf.currentTerm, DPersist, "Read persist: %s", rf.stateToString())
}

func (rf *Raft) stateToString() string {
	return fmt.Sprintf("T%d, voteFor: %d, Log: [0: %d)", rf.currentTerm, rf.voteFor, rf.log.size())
}
