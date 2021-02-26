package personas

import (
	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/redis"
)

type Storage interface {
	Load(id string) (data []byte, err error)
	Set(id string, property uint16, value uint8) (data []byte, err error)
	Get(id string, property uint16) (exists bool, err error)
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

func (r *redisStorage) Set(id string, property uint16, value uint8) (data []byte, err error) {
	var pool *redis.Pool
	pool, err = r.driver.Get(id)
	if err != nil {
		return
	}

	red := pool.Get()
	defer pool.Put(red)

	key := []byte(r.prefix + id)
	var results []interface{}
	results, err = red.Multi().
		Send("SETBIT", key, int(property), value).
		Send("GET", key).
		ExecPipeline(red)
	if err != nil {
		return
	}
	return results[1].([]byte), nil
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
