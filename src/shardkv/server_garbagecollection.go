package shardkv

import (
	"sync"
	"time"
)

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
		// 旧config上的shard已经被删除，不用再重复删除
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
