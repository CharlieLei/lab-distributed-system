package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(dClient, "S%d:T%d {%v,cIdx%d,lApp%d,1Log%v,-1Log%v} BEFORE AppArgs%v, AppRply%v",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.changeState(StateFollower)
	rf.electionTimer.Reset(randomElectionTimeout())

	// 此时args.Term >= rf.currentTerm，该HeartBeat有效
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Term, reply.Success = rf.currentTerm, false
		firstIdx, lastIdx := rf.getFirstLog().Index, rf.getLastLog().Index
		if args.PrevLogIndex > lastIdx {
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIdx+1
		} else {
			conflictTerm := rf.getLog(args.PrevLogIndex).Term
			conflictIdx := args.PrevLogIndex
			for conflictIdx > firstIdx && rf.getLog(conflictIdx-1).Term == conflictTerm {
				conflictIdx--
			}
			reply.ConflictTerm, reply.ConflictIndex = conflictTerm, conflictIdx
		}
		return
	}

	// log[0: prevLogIndex+1)的日志项都与leader相同
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// 检查log[prevLogIndex+1, ...)中是否有与传入的entries[]有冲突的部分
	firstIdx := rf.getFirstLog().Index
	for entryIdx, entry := range args.Entries {
		if entry.Index-firstIdx >= len(rf.logs) || rf.getLog(entry.Index).Term != entry.Term {
			// append any new entries not already in the log
			// entries超出log的部分也可以看作conflict，从而合并”部分match“和”没有一个match“的情况
			rf.logs = rf.getLogSlice(firstIdx, entry.Index)
			rf.logs = append(rf.logs, args.Entries[entryIdx:]...)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
		rf.applyCond.Signal()
	}

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) matchLog(leaderPrevLogTerm, leaderPrevLogIdx int) bool {
	lastIdx := rf.getLastLog().Index
	return leaderPrevLogIdx <= lastIdx && rf.getLog(leaderPrevLogIdx).Term == leaderPrevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
