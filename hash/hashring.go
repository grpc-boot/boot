package hash

import (
	"errors"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

var ErrNoServer = errors.New("no server")

type Ring interface {
	StoreServers(servers []CanHash)
	Get(key []byte) (server CanHash, err error)
	AddServer(server CanHash)
	RemoveServer(server CanHash)
	Length() int
	Range(handler func(index int, server CanHash) (handled bool))
}

type CanHash interface {
	HashCode() (hashValue uint32)
}

type Node struct {
	server    CanHash
	hashValue uint32
}

type NodeList []Node

func (n NodeList) Len() int {
	return len(n)
}

func (n NodeList) Less(i, j int) bool {
	return n[i].hashValue < n[j].hashValue
}

func (n NodeList) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

type DefaultRing struct {
	Ring

	nodes NodeList
	mutex sync.RWMutex
}

func NewDefaultRing(servers []CanHash) *DefaultRing {
	ring := &DefaultRing{}
	ring.StoreServers(servers)
	return ring
}

func (h *DefaultRing) StoreServers(servers []CanHash) {
	h.mutex.Lock()
	nodes := make(NodeList, len(servers), len(servers))

	for index, _ := range servers {
		nodes[index] = Node{
			server:    servers[index],
			hashValue: servers[index].HashCode(),
		}
	}

	h.nodes = nodes
	sort.Sort(h.nodes)

	h.mutex.Unlock()
}

func (h *DefaultRing) AddServer(server CanHash) {
	h.mutex.Lock()
	h.nodes = append(h.nodes, Node{
		server:    server,
		hashValue: server.HashCode(),
	})
	sort.Sort(h.nodes)
	h.mutex.Unlock()
}

func (h *DefaultRing) RemoveServer(server CanHash) {
	h.mutex.Lock()

	value := server.HashCode()

	index := sort.Search(len(h.nodes), func(i int) bool {
		return h.nodes[i].hashValue >= value
	})

	if index < len(h.nodes) && h.nodes[index].hashValue == value {
		if len(h.nodes) == 1 {
			h.nodes = h.nodes[:0]
		} else {
			h.nodes = append(h.nodes[0:index], h.nodes[index+1:len(h.nodes)]...)
			sort.Sort(h.nodes)
		}
	}

	h.mutex.Unlock()
}

func (h *DefaultRing) Get(key []byte) (server CanHash, err error) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	length := len(h.nodes)
	if length == 0 {
		return nil, ErrNoServer
	}

	if length < 1 {
		return h.nodes[0].server, nil
	}

	value := crc32.ChecksumIEEE(key)
	index := sort.Search(length, func(i int) bool {
		return h.nodes[i].hashValue >= value
	})

	if index == length || index == 0 {
		if (value - h.nodes[length-1].hashValue) < (math.MaxUint32 - value + h.nodes[0].hashValue) {
			return h.nodes[length-1].server, nil
		}
		return h.nodes[0].server, nil
	}

	if (h.nodes[index].hashValue - value) > (value - h.nodes[index-1].hashValue) {
		return h.nodes[index-1].server, nil
	}

	return h.nodes[index].server, nil
}

func (h *DefaultRing) Length() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return len(h.nodes)
}

func (h *DefaultRing) Range(handler func(index int, server CanHash) (handled bool)) {
	h.mutex.RLock()
	for index, _ := range h.nodes {
		handler(index, h.nodes[index].server)
	}
	h.mutex.RUnlock()
}
