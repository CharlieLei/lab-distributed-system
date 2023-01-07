package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/debug"
	"6.824/labrpc"
	"6.824/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	sequenceNum int
	leaderIds   map[int]int // gid -> leaderId
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderIds = make(map[int]int)
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := &OperationArgs{}
	args.Op, args.Key = OpGet, key
	return ck.sendCommand(args)
}

func (ck *Clerk) Put(key string, value string) {
	args := &OperationArgs{}
	args.Op, args.Key, args.Value = OpPut, key, value
	ck.sendCommand(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &OperationArgs{}
	args.Op, args.Key, args.Value = OpAppend, key, value
	ck.sendCommand(args)
}

func (ck *Clerk) sendCommand(args *OperationArgs) string {
	args.ClientId, args.SequenceNum = ck.clientId, ck.sequenceNum
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			leaderId := ck.leaderIds[gid]
			for range servers {
				var reply CommandReply
				srv := ck.make_end(servers[leaderId])
				ok := srv.Call("ShardKV.ExecOperation", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					debug.Debug(debug.KVClient, "C%d Send Command To G%d:S%d Success args %v, rply {%v, %v}",
						ck.clientId, gid, leaderId, args, reply.Err, reply.Value)
					ck.sequenceNum++
					ck.leaderIds[gid] = leaderId
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					break
				}
				leaderId = (leaderId + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
