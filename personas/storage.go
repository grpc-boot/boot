package personas

import (
	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/redis"
)

type Storage interface {
	Load(id string) (data []byte, err error)
	Init(id string, maxLength uint16) (data []byte, err error)
	Set(id string, property uint16, value uint8) (ok bool, err error)
	Get(id string, property uint16) (exists bool, err error)
	Del(id string) (ok bool, err error)
}

type redisStorage struct {
	driver *redis.Group
	prefix string
}

func NewRedisPersonas(driver *redis.Group, prefix string) (storage Storage) {
	if prefix == "" {
		prefix = boot.DefaultPersonasRedisPrefix
	}

	return &redisStorage{
		driver: driver,
		prefix: prefix,
	}
}

func (r *redisStorage) Load(id string) (data []byte, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	return red.Get([]byte(r.prefix + id))
}

func (r *redisStorage) Init(id string, maxLength uint16) (data []byte, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	key := []byte(r.prefix + id)
	_, err = red.SetRange([]byte(r.prefix+id), int(maxLength), []byte{byte(0)})
	if err != nil {
		return
	}
	return red.Get(key)
}

func (r *redisStorage) Set(id string, property uint16, value uint8) (ok bool, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	return red.SetBit([]byte(r.prefix+id), int(property), int(value))
}

func (r *redisStorage) Get(id string, property uint16) (exists bool, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	var val int
	val, err = red.GetBit([]byte(r.prefix+id), int(property))
	return val == 1, err
}

func (r *redisStorage) Del(id string) (ok bool, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)
	_, err = red.Del([]byte(r.prefix + id))

	return err == nil, err
}
