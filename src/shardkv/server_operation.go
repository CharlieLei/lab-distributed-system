package shardkv

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
