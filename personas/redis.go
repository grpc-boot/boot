package personas

import (
	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/redis"
)

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

func (r *redisStorage) Set(id string, property uint16, value bool) (ok bool, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	key := []byte(r.prefix + id)
	if value {
		return red.SetBit(key, int(property), 1)
	}

	return red.SetBit(key, int(property), 0)
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

func (r *redisStorage) Destroy(id string) (ok bool, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	_, err = red.Del(id)
	if err != nil {
		return false, err
	}

	return true, err
}
