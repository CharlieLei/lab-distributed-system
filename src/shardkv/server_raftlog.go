package shardkv

func (kv *ShardKV) checkLogEntryAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(Command{CmdEmptyEntry, nil}, &CommandReply{})
	}
}

func (kv *ShardKV) applyEmptyEntry() *CommandReply {
	return &CommandReply{OK, ""}
}
