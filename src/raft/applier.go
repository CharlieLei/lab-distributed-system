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
			Debug(dError, "S%d:T%d {%v,cIdx%d,lApp%d,1Log%v,-1Log%v} START WAIT",
				rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
			rf.applyCond.Wait()
			Debug(dError, "S%d:T%d {%v,cIdx%d,lApp%d,1Log%v,-1Log%v} GET SIGNAL",
				rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
		}
		firstIdx, commitIdx, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIdx-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIdx:commitIdx+1-firstIdx]) // logs[lastApplied+1,...,commitIdx]
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			Debug(dCommit, "S%d:T%d Apply Entry%v",
				rf.me, rf.currentTerm, entry)
		}
		rf.mu.Lock()
		Debug(dCommit, "S%d:T%d Apply Entry[%d:%d]",
			rf.me, rf.currentTerm, lastApplied+1, commitIdx)
		rf.lastApplied = commitIdx // 可能此时的rf.commitIdx已经改变，与commitIdx变量中的值不同
		rf.mu.Unlock()
	}
}
