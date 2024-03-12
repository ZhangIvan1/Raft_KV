package raft

// Used to apply all logs received by itself, whether leader or follower
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		//	build all ApplyMsg for apply
		rf.mu.Lock()
		rf.applyCond.Wait() //release until signal from leader
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log.at(i))
		}
		rf.mu.Unlock()

		// apply all msg
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}

		}

		// update lastApplied
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply logs for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
