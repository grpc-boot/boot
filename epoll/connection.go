package epoll

import (
	"net"
	"sync"
	"time"

	"github.com/grpc-boot/boot/atomic"
)

type Connection struct {
	fd               int
	addr             net.Addr
	data             map[string]interface{}
	mutex            sync.RWMutex
	latestAccessTime atomic.Int64
}

func newConnection(fd int) (conn *Connection) {
	return &Connection{
		fd:   fd,
		data: make(map[string]interface{}, 8),
	}
}

func (c *Connection) SetValue(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.data[key] = value
}

func (c *Connection) GetValue(key string) (value interface{}, exits bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	value, exits = c.data[key]
	return
}

func (c *Connection) DelValue(keys ...string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, key := range keys {
		delete(c.data, key)
	}
}

func (c *Connection) Access() {
	c.latestAccessTime.Set(time.Now().Unix())
}

func (c *Connection) GetLatestAccessTime() (latestAccessTime int64) {
	return c.latestAccessTime.Get()
}

func (c *Connection) Close() (err error) {
	return
}
