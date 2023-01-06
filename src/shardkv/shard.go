package shardkv

import "fmt"

type ShardStatus string

const (
	INVALID   ShardStatus = "invalid" // 只用于初始化
	WORKING   ShardStatus = "working"
	PULLING   ShardStatus = "pulling"
	BEPULLING ShardStatus = "bepulling" // 该shard已被移出server所在的raft组，但新的raft组还没有获取该shard
	GCING     ShardStatus = "gcing"     // 该shard已经从其他group中获取，需要删除其他group中的该shard
)

type Shard struct {
	Status ShardStatus
	KV     map[string]string
}

func NewShard() *Shard {
	return &Shard{INVALID, make(map[string]string)}
}

func (shard *Shard) Get(key string) (string, ErrType) {
	if val, ok := shard.KV[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key string, value string) ErrType {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key string, value string) ErrType {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deepcopyKV() map[string]string {
	newKV := make(map[string]string)
	for k, v := range shard.KV {
		newKV[k] = v
	}
	return newKV
}

func (shard *Shard) clear() {
	shard.KV = make(map[string]string)
}

func (shard *Shard) String() string {
	return fmt.Sprintf("(%v, %d)", string(shard.Status), len(shard.KV))
}
