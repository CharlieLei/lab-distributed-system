package shardkv

import "time"

func (kv *ShardKV) logEntryChecker() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if !kv.rf.HasLogInCurrentTerm() {
				kv.Execute(Command{CmdEmptyEntry, nil}, &CommandReply{})
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (kv *ShardKV) applyEmptyEntry() CommandReply {
	return CommandReply{OK, ""}
}
