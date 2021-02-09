package container

import (
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
)

type Map struct {
	shardList [256]shard
	length    int64
}

func NewMap() *Map {
	m := &Map{
		shardList: [256]shard{},
	}

	for index := 0; index < math.MaxUint8; index++ {
		m.shardList[index] = shard{
			items: make(map[string]interface{}, 0),
		}
	}

	return m
}

func (m *Map) index(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) & math.MaxUint8
}

func (m *Map) Set(key string, value interface{}) {
	exists := m.shardList[m.index(key)].set(key, value)
	if !exists {
		atomic.AddInt64(&m.length, 1)
	}
}

func (m *Map) Get(key string) (value interface{}, exists bool) {
	return m.shardList[m.index(key)].get(key)
}

func (m *Map) Exists(key string) (exists bool) {
	return m.shardList[m.index(key)].exists(key)
}

func (m *Map) Delete(key string) {
	if exists := m.shardList[m.index(key)].delete(key); exists {
		atomic.AddInt64(&m.length, -1)
	}
}

func (m *Map) Length() int64 {
	return atomic.LoadInt64(&m.length)
}

type shard struct {
	mutex sync.RWMutex
	items map[string]interface{}
}

func (s *shard) set(key string, value interface{}) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists = s.items[key]
	s.items[key] = value
	return
}

func (s *shard) exists(key string) (exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, exists = s.items[key]
	return
}

func (s *shard) get(key string) (value interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, exists = s.items[key]
	return
}

func (s *shard) delete(key string) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists = s.items[key]
	if exists {
		delete(s.items, key)
	}
	return
}
