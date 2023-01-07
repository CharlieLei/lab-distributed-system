package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId    int64
	sequenceNum int // 当前最后一个已发送command的id
	leaderId    int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	// Your code here.
	args.Op, args.Num = OpQuery, num
	return ck.sendCommand(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	// Your code here.
	args.Op, args.Servers = OpJoin, servers
	ck.sendCommand(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	// Your code here.
	args.Op, args.GIDs = OpLeave, gids
	ck.sendCommand(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	// Your code here.
	args.Op, args.Shard, args.GID = OpMove, shard, gid
	ck.sendCommand(args)
}

func (ck *Clerk) sendCommand(args *CommandArgs) Config {
	args.ClientId, args.SequenceNum = ck.clientId, ck.sequenceNum
	for {
		// try each known server.
		for range ck.servers {
			var reply CommandReply
			srv := ck.servers[ck.leaderId]
			ok := srv.Call("ShardCtrler.ExecCommand", args, &reply)
			if ok && reply.Err == OK {
				ck.sequenceNum++
				return reply.Config
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
