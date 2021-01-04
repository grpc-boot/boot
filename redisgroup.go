package boot

import (
	"hash/crc32"
)

type RedisGroup struct {
	pool []*RedisPool
	size uint32
}

func NewGroup(options []RedisOption) *RedisGroup {
	poolSize := len(options)
	if poolSize < 1 {
		panic("redis options长度小于1")
	}

	group := &RedisGroup{
		size: uint32(poolSize),
		pool: make([]*RedisPool, 0, poolSize),
	}

	for index := 0; index < poolSize; index++ {
		group.pool = append(group.pool, NewPool(&options[index]))
	}

	return group
}

func (rg *RedisGroup) Get(key []byte) *RedisPool {
	if rg.size < 2 {
		return rg.pool[0]
	}
	index := crc32.ChecksumIEEE(key) % rg.size
	return rg.pool[index]
}
