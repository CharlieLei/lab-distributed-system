# Lab4 ShardKV

## Lab4A

照抄lab3的实现，且不需要snapshot

为了将`Join`，`Leave`，`Move`，`Query`操作使用统一的rpc函数，rpc参数实现如下：

```go
type OpType string

const (
    OpJoin  OpType = "Join"
    OpLeave OpType = "Leave"
    OpMove  OpType = "Move"
    OpQuery OpType = "Query"
)

type CommandArgs struct {
    ClientId    int64
    SequenceNum int // 就是command的id
    Op          OpType
    Servers     map[int][]string // for Join, new GID -> servers mappings
    GIDs        []int            // for Leave
    Shard       int              // for Move
    GID         int              // for Move
    Num         int              // for Query, desired config number
}

func (args *CommandArgs) isReadOnly() bool {
    return args.Op == OpQuery
}

type CommandReply struct {
    Err    ErrType
    Config Config
}
```

对于执行这四种操作的函数，`Move`和`Query`就是直接实现：

```go
func (sc *ShardCtrler) Move(shard int, gid int) ErrType {
    newCfg := sc.configs[len(sc.configs)-1].copy()
    newCfg.Num++
    newCfg.Shards[shard] = gid
    sc.configs = append(sc.configs, newCfg)
    return OK
}

func (sc *ShardCtrler) Query(num int) (ErrType, Config) {
    queryIdx := num
    if num == -1 || num >= len(sc.configs) {
        queryIdx = len(sc.configs) - 1
    }
    return OK, sc.configs[queryIdx]
}
```

而对于`Join`和`Leave`，由于要求在增删raft组后需要将shard平均分配到raft组且尽量产生较少的shard迁移任务。因此给`Config`类增加`reAllocateGid()`，首先根据`config.Groups`将未分配的shard放入组0；然后找出拥有最多shard和最少shard的raft组，将前者中的一个shard分给后者，不断循环，直到它们之间的差值≤1。

```go
func (sc *ShardCtrler) Join(groups map[int][]string) ErrType {
    newCfg := sc.configs[len(sc.configs)-1].copy()
    newCfg.Num++
    for groupId, shards := range groups {
        newCfg.Groups[groupId] = shards
    }
    newCfg.reAllocateGid()
    sc.configs = append(sc.configs, newCfg)
    return OK
}

func (sc *ShardCtrler) Leave(gids []int) ErrType {
    newCfg := sc.configs[len(sc.configs)-1].copy()
    newCfg.Num++
    for _, groupId := range gids {
        delete(newCfg.Groups, groupId)
    }
    newCfg.reAllocateGid()
    sc.configs = append(sc.configs, newCfg)
    return OK
}

func (cfg *Config) reAllocateGid() {
    for shardId, groupId := range cfg.Shards {
        if _, ok := cfg.Groups[groupId]; !ok {
            // 该分片对应的group已经被移除了，group 0中的分片属于未分配的
            cfg.Shards[shardId] = 0
        }
    }

    if len(cfg.Groups) == 0 {
        return
    }

    // 找出每个group中的分片
    group2shards := make(map[int][]int)
    group2shards[0] = make([]int, 0)
    for groupId := range cfg.Groups {
        group2shards[groupId] = make([]int, 0)
    }
    for shardId, groupId := range cfg.Shards {
        group2shards[groupId] = append(group2shards[groupId], shardId)
    }

    for {
        srcGid, tgtGid := getGidWithMaxShards(group2shards), getGidWithMinShards(group2shards)
        if srcGid != 0 && len(group2shards[srcGid])-len(group2shards[tgtGid]) <= 1 {
            break
        }
        group2shards[tgtGid] = append(group2shards[tgtGid], group2shards[srcGid][0])
        group2shards[srcGid] = group2shards[srcGid][1:]
    }

    var newShards [NShards]int
    for groupId, shards := range group2shards {
        for _, shard := range shards {
            newShards[shard] = groupId
        }
    }

    cfg.Shards = newShards
}
```

需要注意的是，由于上述命令会在一个raft组内的所有节点上执行，因此需要保证`Join`和`Leave`在不同节点上能对shard有相同的分配。但go中map的遍历是不确定的，因此需要对结果进行排序确保每次找到的最多shard和最少shard的raft组在不同节点上是相同的。

```go
func getGidWithMinShards(group2shards map[int][]int) int {
    var groups []int
    for groupId := range group2shards {
        groups = append(groups, groupId)
    }
    // 确保map的遍历结果是确定性的
    sort.Ints(groups)
    minGid, minCnt := -1, NShards+1
    for _, groupId := range groups {
        if groupId != 0 && len(group2shards[groupId]) < minCnt {
            minGid, minCnt = groupId, len(group2shards[groupId])
        }
    }
    return minGid
}

func getGidWithMaxShards(group2shards map[int][]int) int {
    // 总是先从group 0中获得分片
    if shards, ok := group2shards[0]; ok && len(shards) > 0 {
        return 0
    }

    var groups []int
    for groupId := range group2shards {
        groups = append(groups, groupId)
    }
    // 确保map的遍历结果是确定性的
    sort.Ints(groups)
    maxGid, maxCnt := -1, -1
    for _, groupId := range groups {
        if len(group2shards[groupId]) > maxCnt {
            // 可能group2shards中只有group 0了，因此不能排除groupId=0的情况
            maxGid, maxCnt = groupId, len(group2shards[groupId])
        }
    }
    return maxGid
}
```



## Lab4B

### 设计

基于Lab3实现，但Lab3的实现只适用于单个raft组，需要添加配置更新、分片迁移后才能满足Lab4B的要求。

首先给出ShardKV类和apply协程的实现

```go
type ShardKV struct {
    mu       sync.Mutex
    me       int
    rf       *raft.Raft
    applyCh  chan raft.ApplyMsg
    make_end func(string) *labrpc.ClientEnd
    gid      int
    ctrlers  []*labrpc.ClientEnd
    dead     int32

    maxraftstate int // snapshot if log grows this big

    mck            *shardctrler.Clerk
    persister      *raft.Persister
    shards         map[int]*Shard // shardId -> pointer of Shard obj, Shard obj == StateMachine
    clientSessions map[int64]Session
    notifyChans    map[int]chan *CommandReply
    lastApplied    int
    previousCfg    *shardctrler.Config
    currentCfg     *shardctrler.Config
}

func (kv *ShardKV) applier() {
    for message := range kv.applyCh {
        if message.CommandValid {
            kv.mu.Lock()

            if kv.lastApplied >= message.CommandIndex {
                kv.mu.Unlock()
                continue
            }
            kv.lastApplied = message.CommandIndex

            var reply *CommandReply
            command := message.Command.(Command)
            switch command.Type {
            case CmdOperation:
                operation := command.Data.(OperationArgs)
                reply = kv.applyOperation(&operation)
            case CmdConfig:
                nextCfg := command.Data.(shardctrler.Config)
                reply = kv.applyConfig(&nextCfg)
            case CmdInsertShards:
                shardsInfo := command.Data.(ShardOperationReply)
                reply = kv.applyInsertShards(&shardsInfo)
            case CmdDeleteShards:
                shardsInfo := command.Data.(ShardOperationArgs)
                reply = kv.applyDeleteShards(&shardsInfo)
            case CmdEmptyEntry:
                reply = kv.applyEmptyEntry()
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
```

由于**配置更新**、**分片迁移**、**垃圾回收**和**检测Raft日志状态**这个任务都需要使用协程执行，并且这些任务都**只能leader**来执行，因此抽取处`monitor()`函数来执行这四个任务。

由于分片数据和配置要相匹配，因此快照需要里需要持久化配置。

```
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
    labgob.Register(Command{})
    labgob.Register(OperationArgs{})
    labgob.Register(shardctrler.Config{})
    labgob.Register(ShardOperationArgs{})
    labgob.Register(ShardOperationReply{})

    applyCh := make(chan raft.ApplyMsg)
    kv := &ShardKV{
        me:           me,
        rf:           raft.Make(servers, me, persister, applyCh),
        applyCh:      applyCh,
        make_end:     make_end,
        gid:          gid,
        ctrlers:      ctrlers,
        maxraftstate: maxraftstate,
        mck:            shardctrler.MakeClerk(ctrlers),
        persister:      persister,
        shards:         make(map[int]*Shard),
        clientSessions: make(map[int64]Session),
        notifyChans:    make(map[int]chan *CommandReply),
        lastApplied:    0,
        previousCfg:    &shardctrler.Config{},
        currentCfg:     &shardctrler.Config{},
    }
    for shardId := range kv.currentCfg.Shards {
        kv.shards[shardId] = NewShard()
    }
    snapshot := persister.ReadSnapshot()
    kv.installSnapshot(snapshot)

    go kv.applier()
    go kv.monitor(kv.updateConfigAction, UpdateConfigTimeout)
    go kv.monitor(kv.migrateShardsAction, MigrateShardsTimeout)
    go kv.monitor(kv.collectGarbageAction, CollectGarbageTimeout)
    go kv.monitor(kv.checkLogEntryAction, CheckLogEntryTimeout)

    return kv
}

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
    for !kv.killed() {
        if _, isLeader := kv.rf.GetState(); isLeader {
            action()
        }
        time.Sleep(timeout)
    }
}
```

##### 命令类型

命令是server向下发送到raft中的内容，一共有5种类型：

- `CmdOperation`：执行Get, Put, Append操作
- `CmdConfig`：配置更新
- `CmdInsertShards`：**分片迁移**任务中向raft组加入日志
- `CmdDeleteShards`：**垃圾回收**任务中要求旧raft组删除日志，要求新raft组将分片状态从`GCING`变为`WORKING`
- `CmdEmptyEntry`：空日志，使server下面的raft达到最新状态

##### 分片类

每个分片类带有存储键值对的散列表和其状态，一共有4种状态：

- `INVALID`：只用于server初始化时各个分片的状态
- `WORKING`：分片的默认状态，如果当前配置下raft组管理该分片则能读写该分片；否则不能读写。根据新配置，分片状态会变为`PULLING`或`WORKING`
- `PULLING`：【当前raft组】需要从在上一配置中管理该分片的【旧raft组】拉取该分片，但还没有获得。在【当前raft组】获取分片后，该分片状态变为`GCING`
- `BEPULLING`：【当前raft组】在上一配置中管理该分片，在当前配置中不再管理，等待【新raft组】拉取该分片
- `GCING`：【当前raft组】已经拉取该分片，但【旧raft组】还没有删除该分片。删除后，【当前raft组】中该分片状态变为`WORKING`；【旧raft组】删除该分片，状态变为`WORKING`

```go
const (
    INVALID   ShardStatus = "invalid" // 只用于初始化
    WORKING   ShardStatus = "working"
    PULLING   ShardStatus = "pulling"
    BEPULLING ShardStatus = "bepulling" // 该shard已被移出server所在的raft组，但新的raft组还没有获取该shard
    GCING     ShardStatus = "gcing"     // 该shard已经从其他group中获取，需要删除其他group中的该shard
)

type Shard struct {
    Status ShardStatus
    KV     map[string]string
}

func NewShard() *Shard {
    return &Shard{INVALID, make(map[string]string)}
}

func (shard *Shard) Get(key string) (string, ErrType) {
    if val, ok := shard.KV[key]; ok {
        return val, OK
    }
    return "", ErrNoKey
}

func (shard *Shard) Put(key string, value string) ErrType {
    shard.KV[key] = value
    return OK
}

func (shard *Shard) Append(key string, value string) ErrType {
    shard.KV[key] += value
    return OK
}

func (shard *Shard) deepcopyKV() map[string]string {
    newKV := make(map[string]string)
    for k, v := range shard.KV {
        newKV[k] = v
    }
    return newKV
}

func (shard *Shard) clear() {
    shard.KV = make(map[string]string)
}
```

##### 读写操作

server收到读写操作请求时，必须要确保以下两个条件同时满足：

- 当前配置中该【raft组】管理该分片
- 该分片的状态是`WORKING`或`GCING`

否则说明【raft组】正在从其他raft组获取该分片，此时不能执行该读写操作。当server向下给raft发送操作和raft向上apply操作时都需要检查能发读写对应的分片。

```go
func (kv *ShardKV) canServe(shardId int) bool {
    return kv.currentCfg.Shards[shardId] == kv.gid &&
        (kv.shards[shardId].Status == WORKING || kv.shards[shardId].Status == GCING)
}

func (kv *ShardKV) ExecOperation(args *OperationArgs, reply *CommandReply) {
    kv.mu.Lock()
    if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
        lastReply := kv.clientSessions[args.ClientId].LastReply
        reply.Err, reply.Value = lastReply.Err, lastReply.Value
        kv.mu.Unlock()
        return
    }

    shardId := key2shard(args.Key)
    if !kv.canServe(shardId) {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    kv.Execute(Command{CmdOperation, *args}, reply)
}

func (kv *ShardKV) applyOperation(args *OperationArgs) *CommandReply {
    var reply CommandReply
    shardId := key2shard(args.Key)
    if kv.canServe(shardId) {
        if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
            lastReply := kv.clientSessions[args.ClientId].LastReply
            reply.Err, reply.Value = lastReply.Err, lastReply.Value
        } else {
            // apply operation to shard
            switch args.Op {
            case OpPut:
                reply.Err = kv.shards[shardId].Put(args.Key, args.Value)
            case OpAppend:
                reply.Err = kv.shards[shardId].Append(args.Key, args.Value)
            case OpGet:
                reply.Value, reply.Err = kv.shards[shardId].Get(args.Key)
            }

            if !args.isReadOnly() {
                kv.clientSessions[args.ClientId] = Session{args.SequenceNum, &reply}
            }
        }
    } else {
        reply.Err = ErrWrongGroup
    }
    return &reply
}
```

##### 配置更新

配置更新协程定期检测所有分片的状态，一旦存在某个分片不为`INVALID`也不为`WORKING`，则说明当前raft还未完成配置更新，此时就**不能**让协程去发送配置更新请求。

**注意1：**必须要**按configNum的顺序逐个**Query，不能直接获得最新配置。因为每次更新配置的命令都会加入raft的日志中，可能某个follower有很多日志项没有apply，如果该follower在其配置很落后时就直接apply了最新配置，则在这两配置之间存在operation项，这些项会对不在这两个配置中的shard进行操作，从而出现问题。因此必须按顺序获取配置，逐个更新。

在apply新配置时，传入的配置可能对应于很早之前的请求，此时传入的配置是之前已经更新过的，因此要检查传入配置的configNum与server的currentCfg的configNum。

```go
func (kv *ShardKV) updateConfigAction() {
    canPullCfg := true
    kv.mu.Lock()
    for _, shard := range kv.shards {
        if shard.Status != WORKING && shard.Status != INVALID {
            canPullCfg = false
            break
        }
    }
    currentCfgNum := kv.currentCfg.Num
    kv.mu.Unlock()
    if canPullCfg {
        nextCfg := kv.mck.Query(currentCfgNum + 1)
        if nextCfg.Num == currentCfgNum+1 {
            kv.Execute(Command{CmdConfig, nextCfg}, &CommandReply{})
        }
    }
}

func (kv *ShardKV) applyConfig(nextCfg *shardctrler.Config) *CommandReply {
    var reply CommandReply
    if nextCfg.Num == kv.currentCfg.Num+1 {
        for shardId := 0; shardId < shardctrler.NShards; shardId++ {
            shard := kv.shards[shardId]
            currGroupId, nextGroupId := kv.currentCfg.Shards[shardId], nextCfg.Shards[shardId]
            if shard.Status == INVALID {
                shard.Status = WORKING
            } else if currGroupId == kv.gid && nextGroupId != kv.gid {
                shard.Status = BEPULLING
            } else if currGroupId != kv.gid && nextGroupId == kv.gid {
                shard.Status = PULLING
            }
        }
        kv.previousCfg = kv.currentCfg
        kv.currentCfg = nextCfg
        reply.Err = OK
    } else {
        reply.Err = ErrOutDated
    }
    return &reply
}
```

##### 分片迁移

分片迁移协程定期检测是否有分片处于`PULLING`状态，利用server保存的上一个配置`previousCfg`来找需要拉取的分片和分片对应的raft组，然后并行拉取分片。

在【旧raft组】收到分片迁移请求时，**只有leader**能进行处理，组内的其他server收到后直接返回`ErrWrongLeader`。

**注意1：**为了避免在【回复请求】和【apply分片迁移数据】时遇到过时的数据，与分片操作的RPC需要需要带上对应配置configNum。检查传入数据的configNum与server当前配置的configNum。但在【旧raft组】回复请求时，**configNum < currentCfg.Num时要正常返回分片数据**。因为每个raft组**按顺序**apply每个配置，因此可能某些raft组落后于其他组，其configNum比其他组小。若把configNum < currentCfg.Num的请求当作已经处理过的请求，则较为落后的【raft组】无法从【其他raft组】获得分片。

**注意2：**在【旧raft组】回复请求时，必须**将server对client的最近一次回复`clientSession`发给【新raft组】**;否则可能会出现如下情况，使得client读写操作重复执行：

- client向shard1写数据

1. 当server写入shard并更新其`clientSessions`后，整个【raft组1】崩溃，因此client没有收到返回信息
2. 此时shard1分配给另外一个raft组2，然后【raft组1】恢复，【raft组2】向【raft组1】获取shard1
3. 由于client没收到返回信息，因此会向组2重新发写数据请求
4. 如果组2没有组1的`clientSession`，组2就会认为client发送的是新请求而不是已经写入的请求，这样就会在shard1上执行两次写

```go
type ShardOperationArgs struct {
    ConfigNum int
    ShardIds  []int
}

type ShardOperationReply struct {
    Err            ErrType
    ConfigNum      int
    ShardsKV       map[int]map[string]string
    ClientSessions map[int64]Session
}

func (kv *ShardKV) migrateShardsAction() {
    kv.mu.Lock()

    groupId2shardIds := kv.getShardIdsByStatus(PULLING)

    var wg sync.WaitGroup
    for groupId, shardIds := range groupId2shardIds {
        wg.Add(1)
        go func(servers []string, configNum int, shardIds []int) {
            defer wg.Done()
            args := ShardOperationArgs{configNum, shardIds}
            for _, server := range servers {
                var reply ShardOperationReply
                srv := kv.make_end(server)
                ok := srv.Call("ShardKV.GetShardsData", &args, &reply)
                if ok && reply.Err == OK {
                    kv.Execute(Command{CmdInsertShards, reply}, &CommandReply{})
                }
            }
        }(kv.previousCfg.Groups[groupId], kv.currentCfg.Num, shardIds)
    }

    kv.mu.Unlock()
    wg.Wait()
}

func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
    if _, isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if args.ConfigNum > kv.currentCfg.Num {
        reply.Err = ErrShardNotReady
        return
    }

    reply.ShardsKV = make(map[int]map[string]string)
    for _, shardId := range args.ShardIds {
        reply.ShardsKV[shardId] = kv.shards[shardId].deepcopyKV()
    }

    reply.ClientSessions = make(map[int64]Session)
    for clientId, session := range kv.clientSessions {
        reply.ClientSessions[clientId] = session.deepcopy()
    }

    reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) *CommandReply {
    var reply CommandReply
    if shardsInfo.ConfigNum == kv.currentCfg.Num {
        for shardId, shardKV := range shardsInfo.ShardsKV {
            shard := kv.shards[shardId]
            if shard.Status == PULLING {
                for k, v := range shardKV {
                    shard.KV[k] = v
                }
                shard.Status = GCING
            } else {
                break
            }
        }
        for clientId, newShardSession := range shardsInfo.ClientSessions {
            oldShardSession, ok := kv.clientSessions[clientId]
            if !ok || oldShardSession.LastSequenceNum < newShardSession.LastSequenceNum {
                kv.clientSessions[clientId] = newShardSession
            }
        }
        reply.Err = OK
    } else {
        reply.Err = ErrOutDated
    }
    return &reply
}
```

##### 垃圾回收

分片迁移协程定期检测是否有分片处于`GCING`状态，利用server保存的上一个配置`previousCfg`来找需要拉取的分片和分片对应的raft组，然后并行拉取分片。因为需要对【新raft组】和【旧raft组】中的分片进行操作，因此新旧raft组都需要向raft发送垃圾回收数据；【旧raft组】在apply数据时，会删除分片；【新raft组】会将处于`GCING`的分片转为`WORKING`，表示垃圾回收完成。

在apply回收数据时，传入的配置可能对应于很早之前的请求，此时传入的配置是之前已经更新过的，因此要检查传入配置的configNum与server的currentCfg的configNum。

```go
func (kv *ShardKV) collectGarbageAction() {
    kv.mu.Lock()

    groupId2shardIds := kv.getShardIdsByStatus(GCING)

    var wg sync.WaitGroup
    for groupId, shardIds := range groupId2shardIds {
        wg.Add(1)
        go func(servers []string, configNum int, shardIds []int) {
            defer wg.Done()
            args := ShardOperationArgs{configNum, shardIds}
            for _, server := range servers {
                var reply ShardOperationReply
                srv := kv.make_end(server)
                ok := srv.Call("ShardKV.DeleteShardsData", &args, &reply)
                if ok && reply.Err == OK {
                    kv.Execute(Command{CmdDeleteShards, args}, &CommandReply{})
                }
            }
        }(kv.previousCfg.Groups[groupId], kv.currentCfg.Num, shardIds)
    }

    kv.mu.Unlock()
    wg.Wait()
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
    if _, isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    if args.ConfigNum > kv.currentCfg.Num {
        reply.Err = ErrShardNotReady
        kv.mu.Unlock()
        return
    } else if args.ConfigNum < kv.currentCfg.Num {
        reply.Err = OK
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    var commandReply CommandReply
    kv.Execute(Command{CmdDeleteShards, *args}, &commandReply)
    reply.Err = commandReply.Err
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationArgs) *CommandReply {
    var reply CommandReply
    if shardsInfo.ConfigNum == kv.currentCfg.Num {
        for _, shardId := range shardsInfo.ShardIds {
            shard := kv.shards[shardId]
            if shard.Status == GCING {
                shard.Status = WORKING
            } else if shard.Status == BEPULLING {
                shard.clear()
                shard.Status = WORKING
            } else {
                break
            }
        }
        reply.Err = OK
    } else {
        reply.Err = OK
    }
    return &reply
}
```

##### 检测Raft日志状态

检测日志协程定期检测raft层的leader是否拥有当前term的日志。如果没有则提交一条空日志，使新leader达到最新状态，避免因Lab2遗留问题带来的活锁。

```go
func (kv *ShardKV) checkLogEntryAction() {
    if !kv.rf.HasLogInCurrentTerm() {
        kv.Execute(Command{CmdEmptyEntry, nil}, &CommandReply{})
    }
}

func (kv *ShardKV) applyEmptyEntry() *CommandReply {
    return &CommandReply{OK, ""}
}
```



### 实现步骤

#### 第1步 基于Lab3实现在单个raft组上运行

照抄lab3的实现，完成后通过TestStaticShards

#### 第2步 实现配置更新

为了便于后续实现分片，构造`Shard`类来存储某个分片的键值对。分片有三个状态：

- `INVALID`：表示分片不可用
- `WORKING`：表示分片可用
- `PULLING`：表示该分片刚加入当前raft组，该分配需要从其他组获得

```go
type Shard struct {
    KV     map[string]string
}

func (shard *Shard) Get(key string) (string, ErrType) {
    if val, ok := shard.KV[key]; ok {
        return val, OK
    }
    return "", ErrNoKey
}

func (shard *Shard) Put(key string, value string) ErrType {
    shard.KV[key] = value
    return OK
}

func (shard *Shard) Append(key string, value string) ErrType {
    shard.KV[key] += value
    return OK
}
```

之后创建一个协程来定期检测所有分片的状态，一旦存在某个分片不为`INVALID`也不为`WORKING`，则说明当前raft还未完成配置更新，此时就不能让协程去发送更新请求。

此外，必须要按configNum的顺序逐个Query，不能直接获得最新配置，因为每次更新配置的命令都会加入raft的日志中，可能某个follower有很多日志项没有apply，如果该follower在其配置很落后时就直接apply了最新配置，则在这两配置之间存在operation项，这些项会对不在这两个配置中的shard进行操作，从而出现问题。因此必须按顺序获取配置，逐个更新。

```go
func (kv *ShardKV) configUpdater() {
    for {
        time.Sleep(80 * time.Millisecond)
        if _, isLeader := kv.rf.GetState(); isLeader {
            canPullCfg := true
            kv.mu.Lock()
            for _, shard := range kv.shards {
                if shard.Status != WORKING && shard.Status != INVALID {
                    canPullCfg = false
                    break
                }
            }
            currentCfgNum := kv.currentCfg.Num
            kv.mu.Unlock()
            if canPullCfg {
                nextCfg := kv.mck.Query(currentCfgNum + 1)
                if nextCfg.Num == currentCfgNum+1 {
                    kv.Execute(Command{CmdConfig, nextCfg}, &CommandReply{})
                }
            }
        }
    }
}
```

在apply新配置时，传入的配置可能对应于很早之前的请求，此时传入的配置是之前已经更新过的，因此要检查传入配置的configNum与server的currentCfg的configNum。

然后，根据新配置来修改各个分片的状态。

```go
func (kv *ShardKV) applyConfig(nextCfg *shardctrler.Config) CommandReply {
    var reply CommandReply
    if nextCfg.Num == kv.currentCfg.Num+1 {
        kv.previousCfg = kv.currentCfg
        kv.currentCfg = nextCfg
        for shardId, groupId := range nextCfg.Shards {
            if kv.shards[shardId].Status == WORKING && groupId != kv.gid {
                kv.shards[shardId].Status = INVALID
            } else if kv.shards[shardId].Status == INVALID && groupId == kv.gid {
                kv.shards[shardId].Status = PULLING
            }
        }
        reply.Err = OK
    } else {
        reply.Err = ErrOutDated
    }
    return reply
}
```

#### 第3步 实现分片迁移

考虑通过如下方式实现分片迁移：让缺少某分片的raft组向拥有该分片的raft组发送请求，从而获得缺少的分片。因此需要实现拉取分片的RPC。server会创建一个协程来定期检测是否有分片需要从其他raft组中获取，如有需要则发送该RPC。

由于只有上一个配置拥有之前该分片所在raft组这一信息，因此server需要保存上一个配置。

```go
func (kv *ShardKV) shardPuller() {
    for {
        if _, isLeader := kv.rf.GetState(); isLeader {
            kv.mu.Lock()

            groupId2shardIds := make(map[int][]int)
            for shardId, shard := range kv.shards {
                if shard.Status == PULLING {
                    targetGroupId := kv.previousCfg.Shards[shardId]
                    groupId2shardIds[targetGroupId] = append(groupId2shardIds[targetGroupId], shardId)
                }
            }

            var wg sync.WaitGroup
            for groupId, shardIds := range groupId2shardIds {
                wg.Add(1)
                go func(servers []string, configNum int, shardIds []int) {
                    defer wg.Done()
                    args := ShardMigrationArgs{configNum, shardIds}
                    for _, server := range servers {
                        var reply ShardMigrationReply
                        srv := kv.make_end(server)
                        ok := srv.Call("ShardKV.GetShardsData", &args, &reply)
                        if ok && reply.Err == OK {
                            kv.Execute(Command{CmdInsertShards, reply}, &CommandReply{})
                        }
                    }
                }(kv.previousCfg.Groups[groupId], kv.currentCfg.Num, shardIds)
            }

            kv.mu.Unlock()
            wg.Wait()
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (kv *ShardKV) GetShardsData(args *ShardMigrationArgs, reply *ShardMigrationReply) {
    if _, isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if args.ConfigNum > kv.currentCfg.Num {
        reply.Err = ErrShardNotReady
        return
    } else if args.ConfigNum < kv.currentCfg.Num {
        reply.Err = ErrOutDated
        return
    }

    reply.ShardsKV = make(map[int]map[string]string)
    for _, shardId := range args.ShardIds {
        reply.ShardsKV[shardId] = kv.shards[shardId].deepcopyKV()
    }

    reply.ConfigNum, reply.Err = args.ConfigNum, OK
}
```

在发送请求并成功返回后，需要将新配置发送到raft中，从而让所在组的所有server都能获得新分片。在raft向上apply新分片后，再将新分片加入。

```
func (kv *ShardKV) applyInsertShards(shardsInfo *ShardMigrationReply) CommandReply {
    var reply CommandReply
    if shardsInfo.ConfigNum == kv.currentCfg.Num {
        for shardId, shardKV := range shardsInfo.ShardsKV {
            shard := kv.shards[shardId]
            if shard.Status == PULLING {
                for k, v := range shardKV {
                    shard.KV[k] = v
                }
                shard.Status = WORKING
            } else {
                break
            }
        }
        reply.Err = OK
    } else {
        panic("ApplyInsertShards different Config num")
    }
    return reply
}
```

当server收到Get, Put, Append操作请求时，必须要确保当前配置中该【raft组】能读写对应的分片，并且该分片的状态是`WORKING`，否则说明【raft组】正在从其他raft组获取该分片，此时不能读写该分片。当server向下给raft发送操作和raft向上apply操作时都需要检查能发读写对应的分片。

```go
func (kv *ShardKV) canServe(shardId int) bool {
    return kv.currentCfg.Shards[shardId] == kv.gid && kv.shards[shardId].Status == WORKING
}

func (kv *ShardKV) ExecOperation(args *OperationArgs, reply *CommandReply) {
    kv.mu.Lock()
    if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
        lastReply := kv.clientSessions[args.ClientId].LastReply
        reply.Err, reply.Value = lastReply.Err, lastReply.Value
        kv.mu.Unlock()
        return
    }

    shardId := key2shard(args.Key)
    if !kv.canServe(shardId) {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    kv.Execute(Command{CmdOperation, *args}, reply)
}

func (kv *ShardKV) applyOperation(args *OperationArgs) CommandReply {
    var reply CommandReply
    shardId := key2shard(args.Key)
    if kv.canServe(shardId) {
        if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
            reply = kv.clientSessions[args.ClientId].LastReply
        } else {
            // apply operation to shard
            switch args.Op {
            case OpPut:
                reply.Err = kv.shards[shardId].Put(args.Key, args.Value)
            case OpAppend:
                reply.Err = kv.shards[shardId].Append(args.Key, args.Value)
            case OpGet:
                reply.Value, reply.Err = kv.shards[shardId].Get(args.Key)
            }

            if !args.isReadOnly() {
                kv.clientSessions[args.ClientId] = Session{args.SequenceNum, reply}
            }
        }
    } else {
        reply.Err = ErrWrongGroup
    }
    return reply
}
```

#### BUG1 configNum较为落后的raft组无法从其他raft组获得分片

由于每个raft组**按顺序**apply每个配置，因此可能某些raft组落后于其他组，其configNum比其他组小。此前认为`GetShardsData()`若收到configNum比raft组当前config的configNum小时，说明该请求是之前多发的。这忽略了存在raft组落后于其他组这一个情况。

修复方法：在`GetShardsData()`中，当configNum < currentCfg.Num时也正常返回分片数据

```go
// GetShardsData()
if args.ConfigNum > kv.currentCfg.Num {
    reply.Err = ErrShardNotReady
    return
}

reply.ShardsKV = make(map[int]map[string]string)
for _, shardId := range args.ShardIds {
    reply.ShardsKV[shardId] = kv.shards[shardId].deepcopyKV()
}
```

修复后通过TestJoinLeave

#### 第4步 处理快照

分片和配置是对应的，若不持久化配置，则在分片恢复后与server当前config不相符合。因此快照需要持久化当前配置和上一个配置。

```go
func (kv *ShardKV) takeSnapshot() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.shards)
    e.Encode(kv.clientSessions)
    e.Encode(kv.previousCfg)
    e.Encode(kv.currentCfg)
    snapshot := w.Bytes()
    return snapshot
}
```

#### BUG2 TestConcurrent3的结果缺少一个字符

发现某些Append请求是重复的，但server没有直接返回，而是执行该请求，从而让结果多Append一次。

原因是没有在获取分片时，一并获取原raft组的`clientSession`。从而忽略了如下情况：当client向shard1写数据

1. 当server写入shard并更新其`clientSessions`后，整个【raft组1】崩溃，因此client没有收到返回信息
2. 此时shard1分配给另外一个raft组2，然后【raft组1】恢复，【raft组2】向【raft组1】获取shard1
3. 由于client没收到返回信息，因此会向组2重新发写数据请求
4. 如果组2没有组1的`clientSession`，组2就会认为client发送的是新请求而不是已经写入的请求，这样就会在shard1上执行两次写

修复方法：在`GetShardsData()`中返回对应raft组的`clientSession`

```go
// GetShardsData()
if args.ConfigNum > kv.currentCfg.Num {
    reply.Err = ErrShardNotReady
    return
}

reply.ShardsKV = make(map[int]map[string]string)
for _, shardId := range args.ShardIds {
    reply.ShardsKV[shardId] = kv.shards[shardId].deepcopyKV()
}

reply.ClientSessions = make(map[int64]Session)
for clientId, session := range kv.clientSessions {
    reply.ClientSessions[clientId] = session.deepcopy()
}
```

修复后通过除TestChallenge1Delete外全部测试

#### 第5步 实现分片垃圾回收

由于只有新raft组知道什么时候完成分片迁移，因此让新raft组在加入新分片后，向旧raft组发送垃圾回收请求，告诉旧raft组可以回收分片。

添加一个新的分片状态`GCING`，表明当前raft组已经获取该分片，但还没有告知旧raft组回收垃圾。创建一个协程来定期检测所有分片的状态，如果存在某个分片状态为`Gcing`时，向旧raft组发送垃圾回收请求。

此外，当新raft组完成分片迁移后，但在旧raft组垃圾回收前的这段时间里，新raft组已经可以读写该分片上的数据了。因此，当分片状态为`GCING`时，也可以读写该分片。从而避免因发送回收请求而降低读写吞吐率。

```go
func (kv *ShardKV) garbageCollector() {
    for {
        if _, isLeader := kv.rf.GetState(); isLeader {
            kv.mu.Lock()

            groupId2shardIds := kv.getShardIdsByStatus(GCING)

            var wg sync.WaitGroup
            for groupId, shardIds := range groupId2shardIds {
                wg.Add(1)
                go func(servers []string, configNum int, shardIds []int) {
                    defer wg.Done()
                    args := ShardOperationArgs{configNum, shardIds}
                    for _, server := range servers {
                        var reply ShardOperationReply
                        srv := kv.make_end(server)
                        ok := srv.Call("ShardKV.DeleteShardsData", &args, &reply)
                        if ok && reply.Err == OK {
                            kv.Execute(Command{CmdDeleteShards, args}, &CommandReply{})
                        }
                    }
                }(kv.previousCfg.Groups[groupId], kv.currentCfg.Num, shardIds)
            }

            kv.mu.Unlock()
            wg.Wait()
        }
        time.Sleep(50 * time.Millisecond)
    }
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
    if _, isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    kv.mu.Lock()
    if args.ConfigNum > kv.currentCfg.Num {
        reply.Err = ErrShardNotReady
        kv.mu.Unlock()
        return
    } else if args.ConfigNum < kv.currentCfg.Num {
        reply.Err = OK
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()

    var commandReply CommandReply
    kv.Execute(Command{CmdDeleteShards, *args}, &commandReply)
    reply.Err = commandReply.Err
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationArgs) CommandReply {
    var reply CommandReply
    if shardsInfo.ConfigNum == kv.currentCfg.Num {
        for _, shardId := range shardsInfo.ShardIds {
            shard := kv.shards[shardId]
            if shard.Status == GCING {
                shard.Status = WORKING
            } else if shard.Status == INVALID {
                shard.clear()
            } else {
                break
            }
        }
        reply.Err = OK
    } else {
        reply.Err = OK
    }
    return reply
}
```

#### 第6步 测试TestChallenge1Delete

发现有时可以过，但大多数都超出内存限制

#### BUG3 在shard未清理的时候更新了配置

原本shard的状态`INVALID`只表示该shard已经移出该raft组，但在实现Challenge1后，状态`INVALID`还包含新的含义：该shard移出原raft组后但还未被新raft组获取。因此，在`configUpdater()`中判断当前是否能更新配置出现了错误。

```go
// configUpdater()
for _, shard := range kv.shards {
    if shard.Status != WORKING && shard.Status != INVALID {
        canPullCfg = false
        break
    }
}
```

例如，G101:S2的shard9出现：

| cfgNum | groupId | status  | ⬇时间轴向下                                                  |
| ------ | ------- | ------- | ------------------------------------------------------------ |
| 10     | 101     | WORKING |                                                              |
| 11     | 102     | INVALID |                                                              |
| 12     | 102     | INVALID |                                                              |
|        |         |         | ←G102发送配置11的delete请求<br />由于G101的cfgNum=12 > 11，G101<br />认为这是过时已经处理过的delete请求<br />，导致G101:S2的shard9没能被删除 |

修复方法：新增并修改shard状态：

- `BEPULLING`，表示该shard已被移出server所在的raft组，但新的raft组还没有获取该shard；
- `WORKING`，只表示该shard可用，但可能该raft组并没有分配到该shard
- `INVALID`，只用在初始化shard，表示该shard什么都没有，不用从其他raft组中获取数据

在`applyConfig()`中，当某个shard状态为`WORKING`时发现新配置中其已被移出raft组，该shard的状态变为`BEPULLING`而不再是`INVALID`。在`applyDeleteShards()`中，状态为`BEPULLING`的shard会变为`INVALID`。

```go
// applyConfig()
for shardId := 0; shardId < shardctrler.NShards; shardId++ {
    shard := kv.shards[shardId]
    currGroupId, nextGroupId := kv.currentCfg.Shards[shardId], nextCfg.Shards[shardId]
    if shard.Status == INVALID {
        shard.Status = WORKING
    } else if currGroupId == kv.gid && nextGroupId != kv.gid {
        shard.Status = BEPULLING
    } else if currGroupId != kv.gid && nextGroupId == kv.gid {
        shard.Status = PULLING
    }
}

// applyDeleteShards()
for _, shardId := range shardsInfo.ShardIds {
    shard := kv.shards[shardId]
    if shard.Status == GCING {
        shard.Status = WORKING
    } else if shard.Status == BEPULLING {
        shard.clear()
        shard.Status = WORKING
    } else {
        break
    }
}
```

#### 第7步 重新执行所有测试

发现TestConcurrent3中出现活锁，例如：

- G101:S1向其他raft组发送`DeleteShardsData()`要求它们删除相应的shard，并且其他组已经返回OK
- 当G101:S1给自己所在的raft组发送`Command`后，在还有server没有apply该`Command`时，G101崩溃
- G101恢复后，G101的leader重新执行上述过程，发现在向自己raft组发送`Command`后，所有server都无法apply

#### BUG4 LAB2遗留问题

该活锁在LAB2B中就已经出现，原因是leader在将entry发送到大多数节点后，还需要这些节点返回消息才能commit。在【已发到大多数节点】和【收到大多数节点的返回消息】之间时，leader可能会崩溃。然后成为leader的节点虽然有该entry，但因为该entry的term小于当前term，所以新leader无法commit该entry。

本应该在leader上任后立刻发送一条空entry，但实现了之后无法通过lab2的测试。

修复方法：在kvserver中不断询问Raft是否有当前term的entry，没有的话发送一条空`Command`，从而实现leader上任发送空entry。

```go
func (kv *ShardKV) logEntryChecker() {
    for {
        if _, isLeader := kv.rf.GetState(); isLeader {
            if !kv.rf.HasLogInCurrentTerm() {
                kv.Execute(Command{CmdEmptyEntry, nil}, &CommandReply{})
            }
        }
        time.Sleep(200 * time.Millisecond)
    }
}

func (kv *ShardKV) applyEmptyEntry() CommandReply {
    return CommandReply{OK, ""}
}
```

