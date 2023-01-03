package shardkv

import (
	"6.824/debug"
	"sync"
	"time"
)

func (kv *ShardKV) shardPuller() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			groupId2shardIds := make(map[int][]int)
			for shardId, shard := range kv.shards {
				if shard.Status == PULLING {
					// 此时的currentCfg已经是新的config，需要从上一个config中获得需要发送的shard所在的group
					targetGroupId := kv.previousCfg.Shards[shardId]
					//if _, hasGroup := kv.previousCfg[targetGroupId]; hasGroup {
					//
					//}
					groupId2shardIds[targetGroupId] = append(groupId2shardIds[targetGroupId], shardId)
				}
			}

			if len(groupId2shardIds) > 0 {
				debug.Debug(debug.KVShard, "S%d:G%d shardPuller, prevCfg %v currCfg %v groupId2shardIds %v kvshards %v",
					kv.me, kv.gid, kv.previousCfg, kv.currentCfg, groupId2shardIds, kv.shards)
			}

			var wg sync.WaitGroup
			for groupId, shardIds := range groupId2shardIds {
				wg.Add(1)
				// CAUTION: 旧的config才有需要pulling的shard所在的group，新的config可能将这个group删除
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					args := ShardMigrationArgs{configNum, shardIds}
					for _, server := range servers {
						var reply ShardMigrationReply
						srv := kv.make_end(server)
						ok := srv.Call("ShardKV.GetShardsData", &args, &reply)
						if ok && reply.Err == OK {
							debug.Debug(debug.KVShard, "S%d:G%d Execute InsertShards, shardInfo %v",
								kv.me, kv.gid, reply)
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
	// 只有group中的leader才能够发送shard
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		debug.Debug(debug.KVShard, "S%d:G%d GetShardsData Finished, args %v rply %v", kv.me, kv.gid, args, reply)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer debug.Debug(debug.KVShard, "S%d:G%d GetShardsData Finished, args %v rply %v", kv.me, kv.gid, args, reply)
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
				// shardPuller可能会发送多个相同configNum的InsertShards命令，这就可能遇到前面的命令已经处理，
				//   后面的命令就会遇到shard.Status == WORKING
				break
			}
		}
		reply.Err = OK
	} else {
		panic("ApplyInsertShards different Config num")
	}
	debug.Debug(debug.KVShard, "S%d:G%d ApplyInsertShards Finished, shardsInfo %v rply %v",
		kv.me, kv.gid, shardsInfo, reply)
	//return CommandReply{ErrOutDated, ""}
	return reply
}
