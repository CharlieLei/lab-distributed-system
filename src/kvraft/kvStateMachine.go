package kvraft

type KVStateMachine struct {
	KVMAP map[string]string
}

func NewKVStateMachine() KVStateMachine {
	rsm := KVStateMachine{}
	rsm.KVMAP = make(map[string]string)
	return rsm
}

func (rsm *KVStateMachine) Get(key string) (ErrType, string) {
	if val, ok := rsm.KVMAP[key]; ok {
		return OK, val
	}
	return OK, ""
}

func (rsm *KVStateMachine) Put(key, value string) ErrType {
	rsm.KVMAP[key] = value
	return OK
}

func (rsm *KVStateMachine) Append(key, value string) ErrType {
	rsm.KVMAP[key] += value
	return OK
}
