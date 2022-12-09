package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type NodeState string

const (
	StateFollower  NodeState = "F"
	StateCandidate NodeState = "C"
	StateLeader    NodeState = "L"
)

const (
	ElectionIntervalRight = 500
	ElectionIntervalLeft  = 150
	HeartbeatInterval     = 50
)

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	state     NodeState

	currentTerm int
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "S%d:T%d Persist State, {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v}",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		Debug(dError, "S%d:T%d Cannot Read Persist", rf.me, rf.currentTerm)
	} else {
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex > rf.getLastLog().Index {
		// 快照比当前节点中的所有日志项都更加新
		rf.logs = make([]Entry, 1)
	} else {
		// 移除比快照旧的日志项
		rf.logs = rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.logs[0].Command = nil
	}
	// 第0个用来记录快照的lastIncludedIndex和lastIncludedTerm
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
	// 被调用CondInstallSnapshot是不具有最新快照的节点，需要保存快照到本地
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	Debug(dSnap, "S%d:T%d {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v} CondInstallSnapshot, AFTER Accept Snapshot{%d %d}",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedIndex, lastIncludedTerm)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIdx := rf.getFirstLog().Index
	if index <= snapshotIdx {
		Debug(dSnap, "S%d:T%d Reject Snapshot %d as Current SnapshotIdx %d is larger",
			rf.me, rf.currentTerm, index, snapshotIdx)
		return
	}
	rf.logs = rf.logs[index-snapshotIdx:] // 第0个用来记录快照的lastIncludedIndex和lastIncludedTerm
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	Debug(dSnap, "S%d:T%d {%v, cIdx%d, lApp%d, 1Log%v, -1Log%v} AFTER Install Snapshot %d",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != StateLeader {
		return -1, -1, false
	} else {
		entry := Entry{command, rf.currentTerm, rf.getLastLog().Index + 1}
		rf.logs = append(rf.logs, entry)
		rf.persist()
		Debug(dLeader, "S%d:T%d Start Entry %v", rf.me, rf.currentTerm, command)
		return rf.getLastLog().Index, rf.currentTerm, true
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		applyCh:        applyCh,
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		commitIndex:    0, // todo
		lastApplied:    0, // todo
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(stableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(randomElectionTimeout()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	// 如果日志有一部分已经转成了快照，则lastApplied应该设为lastIncludedIndex，若从0开始则会在apply日志项时出现越界
	// logs的第0项是dummy，用来存快照的lastIncludedIndex和lastIncludedTerm
	rf.commitIndex, rf.lastApplied = rf.logs[0].Index, rf.logs[0].Index

	Debug(dInfo, "S%d:T%d {%v,cIdx %d,lApp %d,1Log %v,-1Log %v} initialize",
		rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to apply committed logs into applyCh
	go rf.applier()

	return rf
}

func (rf *Raft) changeState(newState NodeState) {
	Debug(dWarn, "S%d:T%d %v -> %v", rf.me, rf.currentTerm, rf.state, newState)
	rf.state = newState
	if newState == StateLeader {
		// initialized to leader last log index + 1
		for idx, _ := range rf.nextIndex {
			rf.nextIndex[idx] = rf.getLastLog().Index + 1
		}
		// initialized to 0
		for idx, _ := range rf.matchIndex {
			rf.matchIndex[idx] = 0
		}
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0] // todo
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1] // todo
}

func (rf *Raft) getLog(index int) Entry {
	return rf.logs[index-rf.getFirstLog().Index]
}

func (rf *Raft) getLogSlice(low, high int) []Entry {
	firstIdx := rf.getFirstLog().Index
	return rf.logs[low-firstIdx : high-firstIdx]
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

func stableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatInterval) * time.Millisecond
}

func randomElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano()) // 避免活锁
	timeout := rand.Intn(ElectionIntervalRight-ElectionIntervalLeft) + ElectionIntervalLeft
	return time.Duration(timeout) * time.Millisecond
}
