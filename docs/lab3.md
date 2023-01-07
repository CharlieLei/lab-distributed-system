# Lab3 KVRaft

主要参考了Raft大论文中的`ClientRequestRPC`

### 实现

#### 客户端

为了避免server重复处理client发送的写请求，所有请求都需要一个标识符。因此，每个client保存自己的`clientId`和`sequenceNum`，`sequenceNum`是该client最后一次成功发送请求的id，每次成功发送后`sequenceNum`加1。这样用`(clientId,sequenceNum)`就可以唯一标识读写请求。

```go
type Clerk struct {
    servers []*labrpc.ClientEnd
    clientId    int64
    sequenceNum int // 当前最后一个已发送command的id
    leaderId    int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    ck.clientId = nrand()
    ck.sequenceNum = 0
    ck.leaderId = 0
    return ck
}

func (ck *Clerk) Get(key string) string {
    args := &CommandArgs{}
    args.Op, args.Key = OpGet, key
    return ck.sendCommand(args)
}
func (ck *Clerk) Put(key string, value string) {
    args := &CommandArgs{}
    args.Op, args.Key, args.Value = OpPut, key, value
    ck.sendCommand(args)
}
func (ck *Clerk) Append(key string, value string) {
    args := &CommandArgs{}
    args.Op, args.Key, args.Value = OpAppend, key, value
    ck.sendCommand(args)
}

func (ck *Clerk) sendCommand(args *CommandArgs) string {
    args.ClientId, args.SequenceNum = ck.clientId, ck.sequenceNum
    debug.Debug(debug.KVClient, "C%d Send Command args %v", ck.clientId, args)
    for {
        // try each known server.
        for range ck.servers {
            var reply CommandReply
            srv := ck.servers[ck.leaderId]
            ok := srv.Call("KVServer.ExecCommand", args, &reply)
            if ok && reply.Err == OK {
                ck.sequenceNum++
                return reply.Value
            }
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
        }
        time.Sleep(100 * time.Millisecond)
    }
}
```

### 服务端

首先给出KVServer类的实现

```go
type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg
    dead    int32 // set by Kill()

    maxraftstate int // snapshot if log grows this big

    persister      *raft.Persister
    stateMachine   KVStateMachine
    clientSessions map[int64]Session
    notifyChans    map[int]chan *CommandReply // 键值是对应日志的index，不是clientId
    lastApplied    int
}
```

#### 复制状态机

受Lab4中分片的启发，此处将存储键值对的散列表打包成一个类。

```go
type KVStateMachine struct {
    KVMAP map[string]string
}

func NewKVStateMachine() KVStateMachine {
    rsm := KVStateMachine{}
    rsm.KVMAP = make(map[string]string)
    return rsm
}

func (rsm *KVStateMachine) Get(key string) (ErrType, string) {
    if val, ok := rsm.KVMAP[key]; ok {
        return OK, val
    }
    return OK, ""
}

func (rsm *KVStateMachine) Put(key, value string) ErrType {
    rsm.KVMAP[key] = value
    return OK
}

func (rsm *KVStateMachine) Append(key, value string) ErrType {
    rsm.KVMAP[key] += value
    return OK
}
```

#### 处理客户端请求

server必须将请求向下发送到Raft层，并从`applyCh`中获得Raft向上apply的数据后，才能将结果返回给client。

因此使用一个channel来沟通client请求处理函数`ExecCommand()`和apply函数`applier()`。当`ExecCommand()`中向Raft层发送数据后便等待channel的结果，并且开一个计时器避免Raft无法处理后server无法返回请求；当`applier()`中Raft层通过`applyCh`将请求数据向上apply后，server处理该请求，并使用channel告知`ExecCommand()`请求结果，让其能返回给client。

**注意1：**不是每个client对应一个channel，是**每个请求**对应一个channel，而每个请求对应Raft层的一个日志，因此可以用Raft日志的index作为channel的id

client可能会发送重复的请求，因此server需要记录每个client最后一次请求的返回结果。client发送请求的`sequenceNum`不可能比server保存的最后一个返回的`sequenceNum`还小；哪怕有，由于client已经发出`sequenceNum`更大的请求，因此client已经不需要收到该回复了。

**注意2：** **避免复制状态机回退**。lab2的文档已经介绍过,follower对于leader发来的snapshot和本地commit的多条日志，在向applyCh中push时无法保证原子性，可能会有snapshot夹杂在多条commit的日志中，如果在kvserver和raft模块都原子性更换状态之后，kvserver又apply了过期的raft日志，则会导致节点间的日志不一致。因此，从applyCh中拿到一个日志后需要保证其index大于等于lastApplied才可以应用到状态机中。

**注意3：** **仅对leader的notifyChan进行通知**。目前的实现中读写请求都需要路由给leader去处理，所以在执行日志到状态机后，只有leader需将执行结果通过`notifyChan`唤醒阻塞的客户端协程，而follower则不需要；对于leader降级为follower的情况，该节点在apply日志后也不能对之前靠index标识的channel进行notify，因为可能执行结果是不对应的，所以在加上只有leader可以notify的判断后，对于此刻还阻塞在该节点的客户端协程，就可以让其自动超时重试。如果读者足够细心，也会发现这里的机制依然可能会有问题，下一点会提到。

**注意4：** **仅对当前term日志的notifyChan进行通知**。上一点提到，对于leader降级为follower的情况，该节点需要让阻塞的请求超时重试以避免违反线性一致性。那么有没有这种可能呢？leader降级为follower后又迅速重新当选了leader，而此时依然有客户端协程未超时在阻塞等待，那么此时apply日志后，根据index获得channel并向其中push执行结果就可能出错，因为可能并不对应。对于这种情况，最直观地解决方案就是仅对当前term日志的`notifyChan`进行通知，让之前term的客户端协程都超时重试即可。

```go
func (kv *KVServer) ExecCommand(args *CommandArgs, reply *CommandReply) {
    kv.mu.Lock()
    if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
        lastReply := kv.clientSessions[args.ClientId].LastReply
        reply.Err, reply.Value = lastReply.Err, lastReply.Value
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    logIdx, _, isLeader := kv.rf.Start(Command{args})
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    ch := kv.getNotifyChan(logIdx)
    kv.mu.Unlock()

    select {
    case result := <-ch:
        reply.Err, reply.Value = result.Err, result.Value
    case <-time.After(ClientRequestTimeout):
        reply.Err = ErrTimeout
    }
    kv.mu.Lock()
    delete(kv.notifyChans, logIdx)
    kv.mu.Unlock()
}

func (kv *KVServer) applier() {
    for kv.killed() == false {
        for message := range kv.applyCh {
            if message.CommandValid {
                kv.mu.Lock()
                command := message.Command.(Command)

                if kv.lastApplied >= message.CommandIndex {
                    kv.mu.Unlock()
                    continue
                }
                kv.lastApplied = message.CommandIndex

                var reply *CommandReply
                if !command.isReadOnly() && kv.isDuplicateRequest(command.ClientId, command.SequenceNum) {
                    reply = kv.clientSessions[command.ClientId].LastReply
                } else {
                    reply = kv.applyLog(&command)
                    if !command.isReadOnly() {
                        kv.clientSessions[command.ClientId] = Session{command.SequenceNum, reply}
                    }
                }

                currentTerm, isLeader := kv.rf.GetState()
                if currentTerm == message.CommandTerm && isLeader {
                    ch := kv.getNotifyChan(message.CommandIndex)
                    ch <- reply
                }

                if kv.needSnapshot() {
                    snapshot := kv.takeSnapshot()
                    kv.rf.Snapshot(message.CommandIndex, snapshot)
                }
                kv.mu.Unlock()
            } else if message.SnapshotValid {
                kv.mu.Lock()
                if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
                    kv.installSnapshot(message.Snapshot)
                    kv.lastApplied = message.SnapshotIndex
                }
                kv.mu.Unlock()
            }
        }
    }
}

func (kv *KVServer) isDuplicateRequest(clientId int64, sequenceNum int) bool {
    session, ok := kv.clientSessions[clientId]
    return ok && sequenceNum <= session.LastSequenceNum
}
```

#### 日志压缩

快照需要持久化状态机和最后一次的回复`clientSession`
