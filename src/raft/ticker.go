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
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	rf.votedFor = rf.me
	grantedVote := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(receiver int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(receiver, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVote += 1
						if grantedVote > len(rf.peers)/2 {
							rf.changeState(StateLeader)
							rf.broadcastHeartbeat()
							rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
						}
					} else if reply.Term > rf.currentTerm {
						rf.changeState(StateFollower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.electionTimer.Reset(randomElectionTimeout())
					} else {
						if !reply.VoteGranted {
							Debug(dDrop, "S%d {T%d} ReqRply from S%d, not grant vote",
								rf.me, rf.currentTerm, receiver)
						} else {
							Debug(dDrop, "S%d {T%d} ReqRply from S%d, reply.Term <= rf.currentTerm",
								rf.me, rf.currentTerm, receiver, reply.Term, rf.currentTerm)
						}
					}
				} else {
					Debug(dWarn, "S%d {T%d} Old ReqRply, rf.currentTerm %d != args.Term %d OR state %v not candidate",
						rf.me, rf.currentTerm, rf.currentTerm, args.Term, rf.state)
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dError, "S%d {T%d} Cannot Send ReqArgs To S%d, ReqArgs%v", rf.me, args.Term, receiver, args)
			}
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		//peerNextIdx := rf.nextIndex[peer]
		go func(receiver int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(receiver, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dError, "S%d {T%d} Cannot Send AppArgs To S%d, AppArgs%v", rf.me, args.Term, receiver, args)
			}
		}(peer)
	}
}
