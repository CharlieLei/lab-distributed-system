package shardkv

import (
	"6.824/debug"
	"6.824/shardctrler"
	"time"
)

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
				// CAUTION: 必须要按顺序逐个Query，不能直接获得最新的config
				//   因为每次更新config的命令都会加入raft的日志中，可能某个follower有很多日志项没有apply，
				//   如果该follower在其config很落后时就直接apply了最新config，则在这两config之间存在operation项，
				//   这些项会对不在这两个config中的shard进行操作，从而出现问题。
				//   因此必须按顺序获取config，逐个更新
				nextCfg := kv.mck.Query(currentCfgNum + 1)
				// 只有更加新的config才需要将新config发送给follower
				if nextCfg.Num == currentCfgNum+1 {
					debug.Debug(debug.KVConfig, "S%d:G%d Exec configUpdate, currCfgNum %v nxtCfgNum %v",
						kv.me, kv.gid, kv.currentCfg.Num, nextCfg.Num)
					kv.Execute(Command{CmdConfig, nextCfg}, &CommandReply{})
				}
			}
		}
	}
}

func (kv *ShardKV) applyConfig(nextCfg *shardctrler.Config) CommandReply {
	// apply的时候可能有很早之前的请求，此时传入的nextCfg是之前已经更新过的
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
	debug.Debug(debug.KVConfig, "S%d:G%d ApplyCfg Finished, prevCfg %v currCfg %v, nxtCfg %v rply %v",
		kv.me, kv.gid, kv.previousCfg, kv.currentCfg, nextCfg, reply)
	return reply
}
