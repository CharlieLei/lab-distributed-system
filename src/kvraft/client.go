package kvraft

import (
	"6.824/debug"
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
	// You will have to modify this struct.
	clientId    int64
	sequenceNum int // 当前最后一个已发送command的id
	leaderId    int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := &CommandArgs{}
	args.Op, args.Key = OpGet, key
	return ck.sendCommand(args)
}
func (ck *Clerk) Put(key string, value string) {
	args := &CommandArgs{}
	args.Op, args.Key, args.Value = OpPut, key, value
	ck.sendCommand(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &CommandArgs{}
	args.Op, args.Key, args.Value = OpAppend, key, value
	ck.sendCommand(args)
}

// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.ExecCommand", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) sendCommand(args *CommandArgs) string {
	args.ClientId, args.SequenceNum = ck.clientId, ck.sequenceNum
	debug.Debug(debug.KVClient, "C%d Send Command args %v", ck.clientId, args)
	for {
		// try each known server.
		for range ck.servers {
			var reply CommandReply
			srv := ck.servers[ck.leaderId]
			ok := srv.Call("KVServer.ExecCommand", args, &reply)
			if ok && reply.Err == OK {
				debug.Debug(debug.KVClient, "C%d Send Command To S%d Success args %v, rply {%v, %v}",
					ck.clientId, ck.leaderId, args, reply.Err, len(reply.Value))
				ck.sequenceNum++
				return reply.Value
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
