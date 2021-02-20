package redis

import (
	"github.com/grpc-boot/boot/hash"
)

type Group struct {
	size uint32
	ring hash.Ring
}

func NewGroup(options []Option, ring hash.Ring) *Group {
	poolSize := len(options)
	if poolSize < 1 {
		panic("redis options长度小于1")
	}

	group := &Group{
		size: uint32(poolSize),
	}

	serverList := make([]hash.CanHash, len(options), len(options))
	for index := 0; index < poolSize; index++ {
		serverList[index] = NewPool(&options[index])
	}

	if ring == nil {
		group.ring = hash.NewDefaultRing(serverList)
	}

	return group
}

func (g *Group) Get(key []byte) (pool *Pool, err error) {
	server, err := g.ring.Get(key)
	if err == nil {
		return server.(*Pool), nil
	}
	return nil, err
}

func (g *Group) Index(i int) (pool *Pool, err error) {
	if i >= g.ring.Length() {
		return nil, hash.ErrNoServer
	}

	g.ring.Range(func(index int, server hash.CanHash) (handled bool) {
		if index == i {
			pool = server.(*Pool)
			return true
		}
		return false
	})
	return
}
