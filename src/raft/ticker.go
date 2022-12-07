package raft

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changeState(StateCandidate)
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTimer.Reset(randomElectionTimeout())
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
						}
					} else if reply.Term > rf.currentTerm {
						rf.changeState(StateFollower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.electionTimer.Reset(randomElectionTimeout())
					}
				}
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

			}
		}(peer)
	}
}
