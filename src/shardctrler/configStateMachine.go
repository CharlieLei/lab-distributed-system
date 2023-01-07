package shardctrler

type ConfigStateMachine struct {
	Configs []Config // indexed by config num
}

func NewConfigStateMachine() ConfigStateMachine {
	cf := ConfigStateMachine{}
	cf.Configs = make([]Config, 1)
	cf.Configs[0].Groups = map[int][]string{}
	return cf
}

func (rsm *ConfigStateMachine) Join(groups map[int][]string) ErrType {
	// Your code here.
	newCfg := rsm.Configs[len(rsm.Configs)-1].copy()
	newCfg.Num++
	for groupId, shards := range groups {
		newCfg.Groups[groupId] = shards
	}
	newCfg.reAllocateGid()
	rsm.Configs = append(rsm.Configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Leave(gids []int) ErrType {
	// Your code here.
	newCfg := rsm.Configs[len(rsm.Configs)-1].copy()
	newCfg.Num++
	for _, groupId := range gids {
		delete(newCfg.Groups, groupId)
	}
	newCfg.reAllocateGid()
	rsm.Configs = append(rsm.Configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Move(shard int, gid int) ErrType {
	// Your code here.
	newCfg := rsm.Configs[len(rsm.Configs)-1].copy()
	newCfg.Num++
	newCfg.Shards[shard] = gid
	rsm.Configs = append(rsm.Configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Query(num int) (ErrType, Config) {
	// Your code here.
	queryIdx := num
	if num == -1 || num >= len(rsm.Configs) {
		queryIdx = len(rsm.Configs) - 1
	}
	return OK, rsm.Configs[queryIdx]
}
