package kvraft

import (
	"6.824/debug"
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.commandId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := &CommandArgs{}
	args.Key, args.Op = key, OpGet
	return ck.sendCommand(args)
}
func (ck *Clerk) Put(key string, value string) {
	args := &CommandArgs{}
	args.Key, args.Value, args.Op = key, value, OpPut
	ck.sendCommand(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &CommandArgs{}
	args.Key, args.Value, args.Op = key, value, OpAppend
	ck.sendCommand(args)
}

// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.ExecCommand", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) sendCommand(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
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
				ck.commandId++
				return reply.Value
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
