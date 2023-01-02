package shardkv

import (
	"6.824/shardctrler"
	"time"
)

func (kv *ShardKV) configPuller() {
	for {
		time.Sleep(80 * time.Millisecond)
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
				kv.Execute(Command{CmdConfig, nextCfg}, &CommandReply{})
			}
		}
	}
}

func (kv *ShardKV) applyConfig(nextCfg *shardctrler.Config) CommandReply {
	// apply的时候可能有很早之前的请求，此时传入的nextCfg是之前已经更新过的
	if nextCfg.Num == kv.currentCfg.Num+1 {
		kv.currentCfg = nextCfg
		for shardId, groupId := range nextCfg.Shards {
			if kv.gid == groupId {
				kv.shards[shardId].Status = WORKING
			} else {
				kv.shards[shardId].Status = INVALID
			}
		}
		return CommandReply{OK, ""}
	}
	return CommandReply{ErrOutDated, ""}
}
