package container

import (
	"hash/crc32"
	"sync"
	"sync/atomic"
)

const (
	maxShard = 255
)

type Map struct {
	shardList [maxShard]shard
	length    int64
}

func NewMap() *Map {
	return &Map{
		shardList: [255]shard{},
	}
}

func (m *Map) index(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key)) & maxShard
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
