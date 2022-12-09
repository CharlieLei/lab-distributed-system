package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		commitIdx, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIdx-lastApplied)
		Debug(dInfo, "S%d:T%d {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v} Applier Copy Entries{len %d}",
			rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), len(entries))
		copy(entries, rf.getLogSlice(lastApplied+1, commitIdx+1)) // logs[lastApplied+1,...,commitIdx]
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		Debug(dCommit, "S%d:T%d Apply Entry[%d:%d]", rf.me, rf.currentTerm, lastApplied+1, commitIdx)
		// CAUTION: 可能此时的rf.commitIdx已经改变，与commitIdx变量中的值不同
		//          也有可能在将ApplyMsg送入applyCh时，该节点收到leader的快照，此快照远比此次apply的日志要新，此时lastApplied已经根据快照修改
		rf.lastApplied = max(commitIdx, rf.lastApplied)
		rf.mu.Unlock()
	}
}
