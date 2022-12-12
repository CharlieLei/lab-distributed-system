package kvraft

import (
	"6.824/debug"
	"6.824/labrpc"
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
	return ck.sendCommand(key, "", OpGet)
}
func (ck *Clerk) Put(key string, value string) {
	ck.sendCommand(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.sendCommand(key, value, OpAppend)
}

// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.ExecCommand", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) sendCommand(key string, value string, op OpType) string {
	args := CommandArgs{
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		Key:       key,
		Value:     value,
		Op:        op,
	}
	debug.Debug(debug.KVClient, "C%d Send Command args %v", ck.clientId, args)
	for {
		var reply CommandReply
		if !ck.servers[ck.leaderId].Call("KVServer.ExecCommand", &args, &reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			//debug.Debug(debug.DWarn, "C%d Send Command To S%d Fail args %v, rply %v",
			//	ck.clientId, ck.leaderId, args, reply)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		debug.Debug(debug.KVClient, "C%d Send Command To S%d Success args %v, rply {%v, %v}",
			ck.clientId, ck.leaderId, args, reply.Err, len(reply.Value))
		ck.commandId++
		return reply.Value
	}
}
