package shardkv

import (
	"6.824/debug"
	"sync"
)

func (kv *ShardKV) collectGarbageAction() {
	kv.mu.Lock()

	groupId2shardIds := kv.getShardIdsByStatus(GCING)

	if len(groupId2shardIds) > 0 {
		debug.Debug(debug.KVGc, "G%d:S%d garbageCollector, prevCfg %v currCfg %v groupId2shardIds %v kvshards %v",
			kv.gid, kv.me, kv.previousCfg, kv.currentCfg, groupId2shardIds, kv.shards)
	}

	var wg sync.WaitGroup
	for groupId, shardIds := range groupId2shardIds {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := ShardOperationArgs{configNum, shardIds}
			for _, server := range servers {
				var reply ShardOperationReply
				srv := kv.make_end(server)
				debug.Debug(debug.KVGc, "G%d:S%d Start DeleteShardsData Send to Server %v, shardOpArgs %v shards %v",
					kv.gid, kv.me, server, args, kv.shards)
				ok := srv.Call("ShardKV.DeleteShardsData", &args, &reply)
				if ok && reply.Err == OK {
					debug.Debug(debug.KVGc, "G%d:S%d Execute DeleteShards, rply %v shards %v",
						kv.gid, kv.me, reply, kv.shards)
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
		debug.Debug(debug.KVGc, "G%d:S%d DeleteShardsData Finished args.ConfigNum %d > currentCfg.Num %d, args %v rply %v shards %v",
			kv.gid, kv.me, args.ConfigNum, kv.currentCfg.Num, args, reply, kv.shards)
		kv.mu.Unlock()
		return
	} else if args.ConfigNum < kv.currentCfg.Num {
		// 旧config上的shard已经被删除，不用再重复删除
		reply.Err = OK
		debug.Debug(debug.KVGc, "G%d:S%d DeleteShardsData Finished args.ConfigNum %d < currentCfg.Num %d, args %v rply %v shards %v",
			kv.gid, kv.me, args.ConfigNum, kv.currentCfg.Num, args, reply, kv.shards)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	debug.Debug(debug.KVGc, "G%d:S%d DeleteShardsData BEFORE Execute, currCfg.Num %d, args %v shards %v",
		kv.gid, kv.me, kv.currentCfg.Num, args, kv.shards)
	kv.mu.Unlock()

	var commandReply CommandReply
	kv.Execute(Command{CmdDeleteShards, *args}, &commandReply)
	reply.Err = commandReply.Err

	kv.mu.Lock()
	debug.Debug(debug.KVGc, "G%d:S%d DeleteShardsData AFTER Execute, currCfg.Num %d, args %v rply %v shards %v",
		kv.gid, kv.me, kv.currentCfg.Num, args, reply, kv.shards)
	kv.mu.Unlock()
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
		debug.Debug(debug.KVGc, "G%d:S%d ApplyDeleteShards Finished, shardsInfo %v shards %v",
			kv.gid, kv.me, shardsInfo, kv.shards)
	} else {
		reply.Err = OK
		debug.Debug(debug.KVGc, "G%d:S%d ApplyDeleteShards shardsInfo.ConfigNum %d != kv.currentCfg.Num %d, shardsInfo %v rply %v",
			kv.gid, kv.me, shardsInfo.ConfigNum, kv.currentCfg.Num, shardsInfo, reply)
	}
	return &reply
}
