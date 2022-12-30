package shardctrler

import (
	"6.824/debug"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type ErrType string

const (
	OK             ErrType = "OK"
	ErrWrongLeader ErrType = "ErrWrongLeader"
	ErrTimeout     ErrType = "ErrTimeout"
)

type OpType string

const (
	OpJoin  OpType = "Join"
	OpLeave OpType = "Leave"
	OpMove  OpType = "Move"
	OpQuery OpType = "Query"
)

type CommandArgs struct {
	ClientId  int64
	CommandId int
	Op        OpType
	Servers   map[int][]string // for Join, new GID -> servers mappings
	GIDs      []int            // for Leave
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // for Query, desired config number
}

type CommandReply struct {
	Err    ErrType
	Config Config
}

func (cfg *Config) copy() Config {
	newConfig := Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range cfg.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
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

	//debug.Debug(debug.KVServer, "CONFIG realloGID, BEFORE group2shards %v", group2shards)
	for {
		srcGid, tgtGid := getGidWithMaxShards(group2shards), getGidWithMinShards(group2shards)
		//debug.Debug(debug.KVServer, "CONFIG realloGID, srcGid %v tgtGid %v group2shards %v", srcGid, tgtGid, group2shards)
		if srcGid != 0 && len(group2shards[srcGid])-len(group2shards[tgtGid]) <= 1 {
			break
		}
		group2shards[tgtGid] = append(group2shards[tgtGid], group2shards[srcGid][0])
		group2shards[srcGid] = group2shards[srcGid][1:]
	}
	//debug.Debug(debug.KVServer, "CONFIG realloGID, AFTER group2shards %v", group2shards)

	var newShards [NShards]int
	for groupId, shards := range group2shards {
		for _, shard := range shards {
			debug.Debug(debug.KVServer, "CONFIG realloGID, groupId %v shards %v shard %v", groupId, shards, shard)
			newShards[shard] = groupId
		}
	}

	cfg.Shards = newShards
}

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
