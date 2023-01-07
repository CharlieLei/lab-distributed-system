package shardctrler

type ConfigStateMachine struct {
	configs []Config // indexed by config num
}

func NewConfigStateMachine() *ConfigStateMachine {
	cf := &ConfigStateMachine{}
	cf.configs = make([]Config, 1)
	cf.configs[0].Groups = map[int][]string{}
	return cf
}

func (rsm *ConfigStateMachine) Join(groups map[int][]string) ErrType {
	// Your code here.
	newCfg := rsm.configs[len(rsm.configs)-1].copy()
	newCfg.Num++
	for groupId, shards := range groups {
		newCfg.Groups[groupId] = shards
	}
	newCfg.reAllocateGid()
	rsm.configs = append(rsm.configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Leave(gids []int) ErrType {
	// Your code here.
	newCfg := rsm.configs[len(rsm.configs)-1].copy()
	newCfg.Num++
	for _, groupId := range gids {
		delete(newCfg.Groups, groupId)
	}
	newCfg.reAllocateGid()
	rsm.configs = append(rsm.configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Move(shard int, gid int) ErrType {
	// Your code here.
	newCfg := rsm.configs[len(rsm.configs)-1].copy()
	newCfg.Num++
	newCfg.Shards[shard] = gid
	rsm.configs = append(rsm.configs, newCfg)
	return OK
}

func (rsm *ConfigStateMachine) Query(num int) (ErrType, Config) {
	// Your code here.
	queryIdx := num
	if num == -1 || num >= len(rsm.configs) {
		queryIdx = len(rsm.configs) - 1
	}
	return OK, rsm.configs[queryIdx]
}
