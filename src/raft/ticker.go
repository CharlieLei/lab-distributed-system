package raft

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == StateFollower || rf.state == StateCandidate {
				rf.changeState(StateCandidate)
				rf.currentTerm += 1
				rf.startElection()
				rf.electionTimer.Reset(randomElectionTimeout())
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.broadcastHeartbeat()
				rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.votedFor = rf.me
	grantedVote := 1
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
						rf.electionTimer.Reset(randomElectionTimeout())
					} else if reply.VoteGranted {
						grantedVote += 1
						if grantedVote > len(rf.peers)/2 {
							rf.changeState(StateLeader)
							rf.broadcastHeartbeat()
							rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
						}
					} else {
						if !reply.VoteGranted {
							Debug(dDrop, "S%d:T%d ReqRply from S%d, not grant vote",
								rf.me, rf.currentTerm, receiver)
						} else {
							Debug(dDrop, "S%d:T%d ReqRply from S%d, reply.Term <= rf.currentTerm",
								rf.me, rf.currentTerm, receiver, reply.Term, rf.currentTerm)
						}
					}
				} else {
					Debug(dWarn, "S%d:T%d Old ReqRply from S%d, rf.currentTerm %d != args.Term %d OR state %v not candidate",
						rf.me, rf.currentTerm, receiver, rf.currentTerm, args.Term, rf.state)
				}
			}
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
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

		go func(receiver int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(receiver, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term {
					if reply.Term > rf.currentTerm {
						// 当前term已经比当前节点所在term大，该节点太久没收到最新heartbeat，仍以为自己是candidate或leader
						rf.changeState(StateFollower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.electionTimer.Reset(randomElectionTimeout())
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
					} else if !reply.Success {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[receiver] = reply.ConflictIndex
						} else {
							termMatchIdx := -1
							for idx := rf.getLastLog().Index + 1; idx > rf.getFirstLog().Index; idx-- {
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
				} else {
					Debug(dWarn, "S%d:T%d Old AppRply, rf.currentTerm %d != args.Term %d",
						rf.me, rf.currentTerm, rf.currentTerm, args.Term)
				}
			}
		}(peer)
	}
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
