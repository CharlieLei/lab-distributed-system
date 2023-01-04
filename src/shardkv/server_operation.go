package shardkv

import "6.824/debug"

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentCfg.Shards[shardId] == kv.gid && kv.shards[shardId].Status == WORKING
}

func (kv *ShardKV) ExecOperation(args *OperationArgs, reply *CommandReply) {
	kv.mu.Lock()
	if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
		lastReply := kv.clientSessions[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		debug.Debug(debug.KVOp, "G%d:S%d ExecOperation Duplicate, clientSessions %v args %v",
			kv.gid, kv.me, kv.clientSessions, args)
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
	debug.Debug(debug.KVOp, "G%d:S%d ApplyOp Finished, args %v rply %v",
		kv.gid, kv.me, args, reply)
	return reply
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, sequenceNum int) bool {
	// 不可能存在比lastCommandId还小；哪怕有，由于client已经发出commandId更大的command，因此client也已经不会接受该回复了
	session, ok := kv.clientSessions[clientId]
	return ok && sequenceNum <= session.LastSequenceNum
}
