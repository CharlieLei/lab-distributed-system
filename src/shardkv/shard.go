package shardkv

type ShardStatus string

const (
	INVALID ShardStatus = "invalid"
	WORKING ShardStatus = "working"
	PULLING ShardStatus = "pulling"
)

type Shard struct {
	Status ShardStatus
	KV     map[string]string
}

func NewShard() *Shard {
	// 避免初始后h第1个
	return &Shard{WORKING, make(map[string]string)}
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

func (shard *Shard) String() string {
	return string(shard.Status)
}
