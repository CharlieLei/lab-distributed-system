package raft

import (
	"6.824/debug"
	"fmt"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) tostring() string {
	return fmt.Sprintf("{%d %d %d %d}", args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer debug.Debug(debug.DSnap, "S%d:T%d {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v} BEFORE SnapArgs%v, AppRply%v",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args.tostring(), reply)

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.changeState(StateFollower)
	rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 安装快照的流程：server将ApplyMsg发给client后，client会调用CondInstallSnapshot，由CondInstallSnapshot来安装传来的最新快照
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
