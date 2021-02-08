package epoll

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

//region 1.0 ConnectionMap
type ConnectionMap struct {
	shards [256]shard
	total  uint32
}

func NewConnectionMap(shardInitSize uint16) *ConnectionMap {
	cmap := &ConnectionMap{
		shards: [256]shard{},
	}

	for index := 0; index <= math.MaxUint8; index++ {
		cmap.shards[index] = shard{
			list: make(map[int]*Connection, shardInitSize),
		}
	}
	return cmap
}

func (cm *ConnectionMap) Add(conn *Connection) {
	if cm.shards[conn.fd&math.MaxUint8].add(conn) == false {
		atomic.AddUint32(&cm.total, 1)
	}
}

func (cm *ConnectionMap) Del(fd int) {
	if cm.shards[fd&math.MaxUint8].del(fd) {
		num := -1
		atomic.AddUint32(&cm.total, uint32(num))
	}
}

func (cm *ConnectionMap) Get(fd int) (conn *Connection, exits bool) {
	return cm.shards[fd&math.MaxUint8].get(fd)
}

func (cm *ConnectionMap) Total() uint32 {
	return atomic.LoadUint32(&cm.total)
}

//endregion

//region 1.1 shard
type shard struct {
	list  map[int]*Connection
	mutex sync.RWMutex
}

func (s *shard) add(conn *Connection) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, exists = s.list[conn.fd]
	s.list[conn.fd] = conn
	return
}

func (s *shard) del(fd int) (exists bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, exists = s.list[fd]
	if exists {
		delete(s.list, fd)
	}
	return
}

func (s *shard) get(fd int) (conn *Connection, exists bool) {
	s.mutex.RUnlock()
	defer s.mutex.RUnlock()

	if conn, exists = s.list[fd]; exists {
		return conn, exists
	}

	return nil, false
}

//endregion

//region 1.2 Connection
type Connection struct {
	fd         int
	reactorId  int
	latestTime int64
	data       map[string]interface{}
	mutex      sync.RWMutex
}

func (c *Connection) Set(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.data[key] = value
}

func (c *Connection) Get(key string) (value interface{}, exits bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	value, exits = c.data[key]
	return
}

func (c *Connection) Del(keys ...string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, key := range keys {
		delete(c.data, key)
	}
}

func (c *Connection) access() {
	atomic.StoreInt64(&c.latestTime, time.Now().Unix())
}

//endregion
