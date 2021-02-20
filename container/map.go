package container

import (
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/hash"
)

type Map struct {
	shardList [256]shard
	length    uint64
}

func NewMap() *Map {
	m := &Map{}
	for index := 0; index < math.MaxUint8; index++ {
		m.shardList[index] = shard{
			items: make(map[interface{}]interface{}, 0),
		}
	}

	return m
}

func (m *Map) index(key interface{}) uint8 {
	switch key.(type) {
	//优先使用自定义hash
	case hash.CanHash:
		return uint8(key.(hash.CanHash).HashCode() & math.MaxUint8)
	case string:
		return uint8(crc32.ChecksumIEEE([]byte(key.(string))) & math.MaxUint8)
	case []byte:
		return uint8(crc32.ChecksumIEEE(key.([]byte)) & math.MaxUint8)
	case uint8:
		return key.(uint8) & math.MaxUint8
	case uint16:
		return uint8(key.(uint16) & math.MaxUint8)
	case uint32:
		return uint8(key.(uint32) & math.MaxUint8)
	case uint64:
		return uint8(key.(uint64) & math.MaxUint8)
	case uint:
		return uint8(key.(uint) & math.MaxUint8)
	case int8:
		return uint8(key.(int8)) & math.MaxUint8
	case int16:
		return uint8(uint16(key.(int16)) & math.MaxUint8)
	case int32:
		return uint8(uint32(key.(int32)) & math.MaxUint8)
	case int64:
		return uint8(uint64(key.(int64)) & math.MaxUint8)
	case int:
		return uint8(uint(key.(int)) & math.MaxUint8)
	}

	return uint8(crc32.ChecksumIEEE([]byte(fmt.Sprintln(key))) & math.MaxUint8)
}

func (m *Map) Set(key interface{}, value interface{}) {
	if exists := m.shardList[m.index(key)].set(key, value); !exists {
		atomic.AddUint64(&m.length, 1)
	}
}

func (m *Map) Get(key interface{}) (value interface{}, exists bool) {
	return m.shardList[m.index(key)].get(key)
}

func (m *Map) Exists(key interface{}) (exists bool) {
	return m.shardList[m.index(key)].exists(key)
}

func (m *Map) Delete(key interface{}) {
	if exists := m.shardList[m.index(key)].delete(key); exists {
		atomic.AddUint64(&m.length, uint64(boot.Decr))
	}
}

func (m *Map) Length() uint64 {
	return atomic.LoadUint64(&m.length)
}

type shard struct {
	mutex sync.RWMutex
	items map[interface{}]interface{}
}

func (s *shard) set(key interface{}, value interface{}) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists = s.items[key]
	s.items[key] = value
	return
}

func (s *shard) exists(key interface{}) (exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, exists = s.items[key]
	return
}

func (s *shard) get(key interface{}) (value interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, exists = s.items[key]
	return
}

func (s *shard) delete(key interface{}) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists = s.items[key]
	if exists {
		delete(s.items, key)
	}
	return
}
