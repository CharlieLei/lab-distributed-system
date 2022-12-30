package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	commandId int
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.commandId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	// Your code here.
	args.Num = num
	args.Op = OpQuery
	return ck.sendCommand(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	// Your code here.
	args.Servers = servers
	args.Op = OpJoin
	ck.sendCommand(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	// Your code here.
	args.GIDs = gids
	args.Op = OpLeave
	ck.sendCommand(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	// Your code here.
	args.Shard, args.GID = shard, gid
	args.Op = OpMove
	ck.sendCommand(args)
}

func (ck *Clerk) sendCommand(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		// try each known server.
		for range ck.servers {
			var reply CommandReply
			srv := ck.servers[ck.leaderId]
			ok := srv.Call("ShardCtrler.ExecCommand", args, &reply)
			if ok && reply.Err != ErrWrongLeader && reply.Err != ErrTimeout {
				ck.commandId++
				return reply.Config
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
