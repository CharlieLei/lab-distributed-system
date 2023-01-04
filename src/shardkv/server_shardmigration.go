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
					groupId2shardIds[targetGroupId] = append(groupId2shardIds[targetGroupId], shardId)
				}
			}

			if len(groupId2shardIds) > 0 {
				debug.Debug(debug.KVShard, "G%d:S%d shardPuller, prevCfg %v currCfg %v groupId2shardIds %v kvshards %v",
					kv.gid, kv.me, kv.previousCfg, kv.currentCfg, groupId2shardIds, kv.shards)
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
						debug.Debug(debug.KVShard, "G%d:S%d Start GetShardsData Send to Server %v, shardMigArgs %v",
							kv.gid, kv.me, server, args)
						ok := srv.Call("ShardKV.GetShardsData", &args, &reply)
						if ok && reply.Err == OK {
							debug.Debug(debug.KVShard, "G%d:S%d Execute InsertShards, shardInfo %v",
								kv.gid, kv.me, reply)
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
		debug.Debug(debug.KVShard, "G%d:S%d GetShardsData Finished, args %v rply %v",
			kv.gid, kv.me, args, reply)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer debug.Debug(debug.KVShard, "G%d:S%d GetShardsData Finished, args %v rply %v",
		kv.gid, kv.me, args, reply)
	if args.ConfigNum > kv.currentCfg.Num {
		reply.Err = ErrShardNotReady
		return
	}
	// CAUTION: 由于server是按照按顺序获取config，因此有可能出现某个group在config上落后很久
	//    此时按顺序像其他group获取shard时，该落后group发送的configNum就会比其他group的configNum小
	//    因此当args.ConfigNum < kv.currentCfg.Num也要正常返回

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
		// CAUTION: 可能会出现如下情况：client向shard1写数据
		//   当server写入shard并更新其clientSessions后，整个raft组1崩溃，因此client没有收到返回信息
		//   此时shard1分配给另外一个raft组2，然后raft组1恢复，raft组2向raft组1获取shard1
		//   由于client没收到返回信息，因此会向组2重新发写数据请求
		//   如果组2没有组1的clientSession，组2就会认为client发送的是新请求而不是已经写入的请求，这样就会在shard1上执行两次写
		for clientId, session := range shardsInfo.ClientSessions {
			lastSession, ok := kv.clientSessions[clientId]
			if !ok || lastSession.LastSequenceNum < session.LastSequenceNum {
				kv.clientSessions[clientId] = session
			}
		}
		reply.Err = OK
	} else {
		panic("ApplyInsertShards different Config num")
	}
	debug.Debug(debug.KVShard, "G%d:S%d ApplyInsertShards Finished, shardsInfo %v rply %v",
		kv.gid, kv.me, shardsInfo, reply)
	//return CommandReply{ErrOutDated, ""}
	return reply
}
