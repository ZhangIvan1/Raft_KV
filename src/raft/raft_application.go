package raft

// Used to apply all logs received by itself, whether leader or follower
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		//	build all ApplyMsg for apply
		rf.mu.Lock()
		rf.applyCond.Wait() //release until signal from leader
		entries := make([]LogEntry, 0)
		snapPendingApply := rf.snapPending

		if !snapPendingApply {
			if rf.lastApplied < rf.log.snapLastIndex {
				rf.lastApplied = rf.log.snapLastIndex
			}

			// make sure that the rf.log have all the entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
			LOG(rf.me, rf.currentTerm, DApply, "applying from %v to %v [normal]", start, end)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "applying from %v to %v [snap]", rf.lastApplied+1, rf.log.snapLastIndex)
		}
		rf.mu.Unlock()

		if !snapPendingApply {
			// apply all msg
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			// apply snapshot
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIndex,
			}
		}

		// update lastApplied
		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply logs for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", rf.log.snapLastIndex)
			rf.lastApplied = rf.log.snapLastIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
