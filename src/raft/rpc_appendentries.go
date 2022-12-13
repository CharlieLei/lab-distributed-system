package raft

import (
	"6.824/debug"
	"fmt"
)

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

func (args *AppendEntriesArgs) tostring() string {
	logstr := "[]"
	if len(args.Entries) == 1 {
		logstr = fmt.Sprintf("[%v]", args.Entries[0])
	} else if len(args.Entries) > 1 {
		logstr = fmt.Sprintf("[%v:%v]", args.Entries[0], args.Entries[len(args.Entries)-1])
	}
	return fmt.Sprintf("{%d %d %d %d %v %d}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, logstr, args.LeaderCommit)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer debug.Debug(debug.DClient, "S%d:T%d {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v} BEFORE AppArgs%v, AppRply%v",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args.tostring(), reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.changeState(StateFollower)
	rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))

	// 此时args.Term >= rf.currentTerm，该HeartBeat有效
	if args.PrevLogIndex < rf.getFirstLog().Index {
		// CAUTION: 在引入快照后，可能出现以下的情况：
		//  leader发送了两次heartbeat，两次heartbeat相同
		//  然而在follower收到并返回第一个heartbeat创建快照，将log截断
		//  在follower收到的第二个heartbeat时，由于该heartbeat中prevLogIndex是旧的，因此prevLogIndex可能会在log被截断的部分中
		//  从而出现错误，第二个heartbeat应该直接返回，并将返回term设为0，让leader认为这是一个过期的回复
		debug.Debug(debug.DWarn, "S%d:T%d Recv Unexpected App From S%d due to prevLogIdx %d < 1Log.Idx %d",
			rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		reply.Term, reply.Success = 0, false
		return
	}

	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Term, reply.Success = rf.currentTerm, false
		firstIdx, lastIdx := rf.getFirstLog().Index, rf.getLastLog().Index
		if args.PrevLogIndex > lastIdx {
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIdx+1
		} else {
			conflictTerm := rf.getLog(args.PrevLogIndex).Term
			conflictIdx := args.PrevLogIndex
			for conflictIdx > firstIdx+1 && rf.getLog(conflictIdx-1).Term == conflictTerm {
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
