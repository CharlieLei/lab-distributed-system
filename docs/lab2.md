# Lab2 Raft

### 0 调试

[Debugging by Pretty Printing (josejg.com)](https://blog.josejg.com/debugging-pretty/)

#### 安装环境

需要安装python的两个包：typer, rich

#### 测试

执行测试并按顺序打印运行结果：

```bash
VERBOSE=1 go test -run <test_test.go中某个测试函数名>
```

执行测试并按顺序和服务器打印运行结果：

```bash
VERBOSE=1 go test -run <test_test.go中某个测试函数名> | python dslogs.py -c <服务器数>
```

并行执行测试多次：

```bash
python dstest.py <test_test.go中某个测试函数名> ...
```

## 1 领袖选举

#### 设计

1. 论文里面没写的是，每个服务器节点需要：
   1. 服务器状态`state`
   2. 两个计时器：`electionExpireTime`，`heartbeatExpireTime`，分别表示论文中的`election timeout`；`leader`发送`AppendEntry`的间隔时间

2. 在初始化服务器节点时，创建协程执行`electionTicker`和`heartbeatTicker`函数，其执行逻辑为休眠一段时间，醒来后处理：
   - `electionTicker`选举领袖：若`electionExpireTime`到期且自身为Follower或Candidate，则修改自己状态，向其他peer发送`RequestVote`请求。
   - `heartbeatTicker`发送心跳：若`heartbeatExpireTime`到期且自身为Leader，则向其他peer发送`AppendEntries`请求。
3. 每个RPC的回复都在发送该RPC的协程中进行处理（例如`RequestVote`的回复就在协程`callForVote`中处理），处理的时候先获得全局的锁，然后修改自己的状态。
   - 若为`callForVote`则收到足够多的票后将自己的状态改为Leader，然后立刻向其他peer发送心跳

#### 注意

在收到RPC请求和回复后都要判断传入的`term`和自身的`currentTerm`之间的关系

1. 在`callForVote`中要检查`RequestVote`返回的消息，因为有可能……
   - `term < currentTerm`：返回的消息对应于之前term所发出的`RequestVote RPC`，该消息就无效了
   - `term > currentTerm`：当前节点错过了之前的heartbeat，没有获得最新的`term`，因此它发出的`RequestVote`是无效的
2. 两个ticker在使用协程调用`callForVote`和`sendHeartBeat`时，都需要直接将参数传入函数，不应该在这两个函数中用`rf`来获得RPC的参数
   - 有可能出现【调用函数时】和【在函数内用`rf`中获得参数时】两者的Raft状态不同
3. 避免活锁
   - 随机生成时加上`rand.Seed(time.Now().UnixNano())`
   - election timeout的上下界要足够大
   - https://www.iditect.com/blog/run-the-mit-6824-raft-experiment-3000-times-without-error.html

```go
type RequestVoteArgs struct {
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

type RequestVoteReply struct {
    Term        int
    VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    if args.Term < rf.currentTerm ||
        (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
        reply.Term, reply.VoteGranted = rf.currentTerm, false
        return
    }
    if args.Term > rf.currentTerm {
        if rf.state == StateLeader {
            rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
        }
        rf.changeState(StateFollower)
        rf.currentTerm, rf.votedFor = args.Term, -1
    }
    if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
        reply.Term, reply.VoteGranted = rf.currentTerm, false
        return
    }
    rf.votedFor = args.CandidateId
    rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))
    reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) isLogUpToDate(candidateLastLogTerm, candidateLastLogIndex int) bool {
    lastLog := rf.getLastLog()
    return candidateLastLogTerm > lastLog.Term ||
        (candidateLastLogTerm == lastLog.Term && candidateLastLogIndex >= lastLog.Index)
}
```




## 2 记录复写

#### 设计

1. 代码中使用`applyCh`来apply已经commit的entry
   - 利用`logEntryApplier`函数来apply；在节点初始化时，创建第3个进程执行`logEntryApplier`，其执行逻辑同`electionTicker`和`heartbeatTicker`
   - Raft类中的`lastApplied`和`commitIndex`可以当作队列的头尾指针，利用它们便可以实现已commit但为apply的entry队列

#### 注意

1. leader commit了最新entry但还没有让其他节点commit该entry时，它有可能收到term值更大的消息，从而让leader变为follower；此时的原leader节点自然无法向其他节点发送消息，使得其他节点无法commit该entry
   - 虽然该节点重新成为leader时，它会在下一次心跳中让其他节点commit该entry，但一旦没重新成为leader就会导致之后的entry无法commit
   - 解决方案：当某个节点被选为leader时都需要commit一个没有操作的entry；但由于测试用例限制apply的commandIndex，因此选择从log队尾往前找，以此更新commitIndex。并且需要在此后有另外一个entry插入才行，若没有则最后这个entry也无法commit
   - [相关讨论1](https://stackoverflow.com/questions/32250879/is-there-a-race-condition-in-raft)， [相关讨论2](https://stackoverflow.com/questions/65230797/how-does-raft-preserve-safty-when-a-leader-commits-a-log-entry-and-crashes-befor)，[相关讨论3](https://stackoverflow.com/questions/37210615/raft-term-condition-to-commit-an-entry)
   - Bug出现于测试TestRPCBytes2B，且**非常隐蔽**
2. leader在更新nextIndex和matchIndex时，有可能……
   - leader在有新的entry后，在给follower发送的heartbeat中会带有该entry，因此AppendEntries中的`entries`不是空而是有内容的
   - 可能follower没有及时回复，超过了heartbeat间隔，从而让leader发送两个有相同`entries`的heartbeat；然后在follower回复后，leader要更新自己的`nextIndex[]`
   - 此时就应该用AppendEntries中的**`prevLogIndex`**来更新`nextIndex[]`，**不能**直接在`nextIndex[]`加上**`entries`的长度**，否则leader的`nextIndex[]`会加上**2**次，出现错误
   - Bug出现于测试TestRPCBytes2B
3. leader在遇到follower的log不一致时，对`nextIndex[]`减少时也会有上面的问题
   - 应该用AppendEntries中的**`prevLogIndex`**来更新`nextIndex[]`，**不能**直接`nextIndex[]--`
4. 注意leader不会修改`matchIndex[]`中自己对应的那一项
   - 在判断是否有大多数节点满足`matchIndex[i] >= N`时，因为leader必然有了该entry，所以计算满足的节点数时要加上**leader自己**
5. 判断大多数节点满足`matchIndex[i] >= N`可以放在leader所发送的`AppendEntries RPC`**成功**时执行
   - 因为只有成功时，`matchIndex[]`中对应接收方的值才会有修改，若没有修改，则不需要做判断

```go
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
    defer rf.persist()

    if args.Term < rf.currentTerm {
        reply.Term, reply.Success = rf.currentTerm, false
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm, rf.votedFor = args.Term, -1
    }
    rf.changeState(StateFollower)
    rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))

    if args.PrevLogIndex < rf.getFirstLog().Index {
        reply.Term, reply.Success = 0, false
        return
    }

    if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
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

    firstIdx := rf.getFirstLog().Index
    for entryIdx, entry := range args.Entries {
        if entry.Index-firstIdx >= len(rf.logs) || rf.getLog(entry.Index).Term != entry.Term {
            rf.logs = rf.getLogSlice(firstIdx, entry.Index)
            rf.logs = append(rf.logs, args.Entries[entryIdx:]...)
        }
    }
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, rf.getLastLog().Index)
        rf.applyCond.Signal()
    }

    reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) matchLog(leaderPrevLogTerm, leaderPrevLogIdx int) bool {
    lastIdx := rf.getLastLog().Index
    return leaderPrevLogIdx <= lastIdx && rf.getLog(leaderPrevLogIdx).Term == leaderPrevLogTerm
}
```



## 3 持久化和日志压缩

加入日志压缩功能后，节点中entry的index和数组下标就不一定匹配了。此外节点还需要记录快照对应的term和index。再考虑到目前entry的第0项是dummy，因此用entry的第0项来记录快照的term和index。

#### Raft客户端要求生成快照

删除对应index前的entry即可

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    snapshotIdx := rf.getFirstLog().Index
    if index <= snapshotIdx {
        return
    }
    rf.logs = rf.logs[index-snapshotIdx:]
    rf.logs[0].Command = nil
    rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}
```

#### leader要求安装快照

当leader定时发送心跳时，leader发现某个follower的`nextIndex`比自己日志中第一个entry的index还小，此时leader只能向该follower发送快照来同步日志。leader通过`InstallSnapshot()`RPC来发送快照。

**注意1：**如果该**快照的`lastIncludedIndex` ≤ follower自身的`commitIndex`**，那说明该节点已经包含了该快照的数据信息。该节点通过Raft客户端调用`Snapshot()`就能获得该快照，不需要安装leader发送过来的这个快照。可能状态机还没有这个快照新，即`lastApplied`还没更新到`commitIndex`，但是`applier()`协程也一定尝试在 apply 了（不可能还没尝试apply这些数据，如果还没尝试apply，则`commitIndex`只可能比当前值要小）。

根据实验的设计，安装快照的流程为：节点将快照数据`ApplyMsg`通过`applyCh`发给Raft客户端后，Raft客户端会调用`CondInstallSnapshot()`，由`CondInstallSnapshot()`来判断是否能安装该快照。

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }
    if args.Term > rf.currentTerm {
        rf.currentTerm, rf.votedFor = args.Term, -1
        rf.persist()
    }
    rf.changeState(StateFollower)
    rf.electionTimer.Reset(randomElectionTimeout(rf.me, rf.currentTerm))

    if args.LastIncludedIndex <= rf.commitIndex {
        reply.Term = rf.currentTerm
        return
    }

    reply.Term = rf.currentTerm
    
    go func() {
        rf.applyCh <- ApplyMsg{
            SnapshotValid: true,
            Snapshot:      args.Data,
            SnapshotTerm:  args.LastIncludedTerm,
            SnapshotIndex: args.LastIncludedIndex,
        }
    }()
}
```

在`CondInstallSnapshot()`中也要判断`lastIncludedIndex <= rf.commitIndex`，不需要leader发来的快照。

此处用entry的第0项来记录快照的term和index。

```go
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if lastIncludedIndex <= rf.commitIndex {
        return false
    }

    if lastIncludedIndex > rf.getLastLog().Index {
        rf.logs = make([]Entry, 1)
    } else {
        rf.logs = rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]
        rf.logs[0].Command = nil
    }
    rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
    rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
    rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
    return true
}
```



## 遗留问题

#### 活锁

问题原因在[记录复写章节的注意部分](#注意-1)的第1条，但在添加空entry后无法通过测试。

