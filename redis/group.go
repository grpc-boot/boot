package redis

import (
	"boot/hash"
)

type Group struct {
	size uint32
	ring *hash.HashRing
}

func NewGroup(options []RedisOption) *Group {
	poolSize := len(options)
	if poolSize < 1 {
		panic("redis options长度小于1")
	}

	group := &Group{
		size: uint32(poolSize),
	}

	serverList := make([]hash.Server, len(options), len(options))
	for index := 0; index < poolSize; index++ {
		serverList[index] = NewPool(&options[index])
	}

	group.ring = hash.NewHashRing(serverList)

	return group
}

func (g *Group) Get(key []byte) (pool *Pool, err error) {
	server, err := g.ring.GetIndex(key)
	if err == nil {
		return server.(*Pool), nil
	}
	return nil, err
}
