package raft

import "6.824/debug"

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == StateFollower || rf.state == StateCandidate {
				debug.Debug(debug.DTimer, "S%d:T%d Election Timer", rf.me, rf.currentTerm)
				rf.changeState(StateCandidate)
				rf.currentTerm += 1
				rf.startElection()
				rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				debug.Debug(debug.DTimer, "S%d:T%d Heartbeat Timer", rf.me, rf.currentTerm)
				rf.broadcastHeartbeat()
				rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	debug.Debug(debug.DVote, "S%d:T%d Start Election, {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v}",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
	rf.votedFor = rf.me
	grantedVote := 1
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLog().Index,
			LastLogTerm:  rf.getLastLog().Term,
		}
		go func(receiver int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(receiver, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.Term > rf.currentTerm {
						rf.changeState(StateFollower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
						rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
					} else if reply.VoteGranted {
						grantedVote += 1
						if grantedVote > len(rf.peers)/2 {
							debug.Debug(debug.DVote, "S%d:T%d Granted Majority Vote", rf.me, rf.currentTerm)
							rf.changeState(StateLeader)
							rf.broadcastHeartbeat()
							rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
						}
					} else {
						if !reply.VoteGranted {
							debug.Debug(debug.DDrop, "S%d:T%d ReqRply from S%d, not grant vote",
								rf.me, rf.currentTerm, receiver)
						} else {
							debug.Debug(debug.DDrop, "S%d:T%d ReqRply from S%d, reply.Term <= rf.currentTerm",
								rf.me, rf.currentTerm, receiver, reply.Term, rf.currentTerm)
						}
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	debug.Debug(debug.DTrace, "S%d:T%d Start Broadcast Heartbeat, {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v}",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.nextIndex[peer] <= rf.getFirstLog().Index {
			// 先发快照，等下一次heartbeat再appendEntries
			debug.Debug(debug.DSnap, "S%d:T%d InstallSnapshot to S%d, nextIndex[%d] %d <= 1Log%v",
				rf.me, rf.currentTerm, peer, peer, rf.nextIndex[peer], rf.getFirstLog())
			rf.installSnapshotHandler(peer)
		} else {
			rf.appendEntriesHandler(peer)
		}
	}
}

func (rf *Raft) appendEntriesHandler(peer int) {
	peerNextIdx := rf.nextIndex[peer]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: peerNextIdx - 1,
		PrevLogTerm:  rf.getLog(peerNextIdx - 1).Term,
		Entries:      make([]Entry, rf.getLastLog().Index-peerNextIdx+1), // 传送rf.logs[nextIndex,...,lastIdx]
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.getLogSlice(peerNextIdx, rf.getLastLog().Index+1))
	debug.Debug(debug.DLeader, "S%d:T%d Send AppRply To S%d, {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v}, args %v",
		rf.me, rf.currentTerm, peer, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args.tostring())

	go func(receiver int) {
		reply := AppendEntriesReply{}
		if rf.sendAppendEntries(receiver, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm == args.Term && rf.state == StateLeader {
				if reply.Term > rf.currentTerm {
					// 当前term已经比当前节点所在term大，该节点太久没收到最新heartbeat，仍以为自己是candidate或leader
					rf.changeState(StateFollower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
					rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
				} else if reply.Success {
					// If successful: update nextIndex and matchIndex for follower
					// CAUTION: 可能该消息返回时，rf.log已经变化了，所以rf.nextIndex不能加len(rf.log)
					//          同样不能直接在rf.nextIndex上加，因为可能leader发送了两个heartbeat要求follower加上新entry，此时就会返回两次；若直接在rf.nextIndex上加，则会加两次
					rf.nextIndex[receiver] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[receiver] = rf.nextIndex[receiver] - 1
					// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
					// and log[N].term == currentTerm: set commitIndex = N
					rf.commitIndex = rf.getNewCommitIndex()
					rf.applyCond.Signal()
					debug.Debug(debug.DLeader, "S%d:T%d Recv Success AppRply, {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v}, args %v, rply %v, nextIdx[%d] = %d",
						rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(),
						args.tostring(), reply, receiver, rf.nextIndex[receiver])
				} else if !reply.Success {
					if reply.ConflictTerm == -1 {
						rf.nextIndex[receiver] = reply.ConflictIndex
					} else {
						termMatchIdx := -1
						for idx := rf.getLastLog().Index + 1; idx > rf.getFirstLog().Index+1; idx-- {
							if rf.getLog(idx-1).Term == reply.ConflictTerm {
								termMatchIdx = idx
								break
							}
						}
						if termMatchIdx == -1 {
							rf.nextIndex[receiver] = reply.ConflictIndex
						} else {
							rf.nextIndex[receiver] = termMatchIdx
						}
					}
				}
			}
		}
	}(peer)
}

func (rf *Raft) getNewCommitIndex() int {
	idx := rf.getLastLog().Index
	for idx >= rf.commitIndex+1 {
		// 从后往前查找，由于每次被选为leader时都会插入一个无操作的entry
		// 因此能避免出现leader commit自己的entry之后被其他节点打断，无法让follower commit的情况
		// 但这样无法通过测试用例对commandIndex的要求
		matchServerCnt := 0
		for peer := range rf.peers {
			if peer == rf.me || rf.matchIndex[peer] >= idx {
				matchServerCnt++
			}
		}
		if matchServerCnt > len(rf.peers)/2 && rf.getLog(idx).Term == rf.currentTerm {
			break
		}
		idx--
	}
	return idx
}

func (rf *Raft) installSnapshotHandler(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getFirstLog().Index,
		LastIncludedTerm:  rf.getFirstLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}

	go func(receiver int) {
		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(receiver, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm == args.Term && rf.state == StateLeader {
				if reply.Term > rf.currentTerm {
					rf.changeState(StateFollower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
					rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
				} else {
					rf.nextIndex[receiver] = max(rf.nextIndex[receiver], args.LastIncludedIndex+1)
				}
			}
		}
	}(peer)
}
