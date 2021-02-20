package redis

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/hash"

	redigo "github.com/garyburd/redigo/redis"
)

type Option struct {
	Host string `yaml:"host" json:"host"`
	Port string `yaml:"port" json:"port"`
	Auth string `yaml:"auth" json:"auth"`
	Db   uint8  `yaml:"db" json:"db"`
	//单位s
	MaxConnLifetime int  `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxIdle         int  `yaml:"maxIdle" json:"maxIdle"`
	MaxActive       int  `yaml:"maxActive" json:"maxActive"`
	Wait            bool `yaml:"wait" json:"wait"`
	//单位ms
	ConnectTimeout int `yaml:"connectTimeout" json:"connectTimeout"`
	//单位ms
	ReadTimeout int `yaml:"readTimeout" json:"readTimeout"`
	//单位ms
	WriteTimeout int `yaml:"writeTimeout" json:"writeTimeout"`
}

type Pool struct {
	hash.CanHash

	id   []byte
	pool *redigo.Pool
}

func NewPool(option *Option) (pool *Pool) {
	return &Pool{
		id: []byte(fmt.Sprintf("%s:%s:%d", option.Host, option.Port, option.Db)),
		pool: &redigo.Pool{
			MaxConnLifetime: time.Second * time.Duration(option.MaxConnLifetime),
			MaxIdle:         option.MaxIdle,
			MaxActive:       option.MaxActive,
			Wait:            option.Wait,
			Dial: func() (redigo.Conn, error) {
				c, err := redigo.Dial("tcp",
					fmt.Sprintf("%s:%s", option.Host, option.Port),
					redigo.DialConnectTimeout(time.Millisecond*time.Duration(option.ConnectTimeout)),
					redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ReadTimeout)),
					redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ReadTimeout)),
				)
				if err != nil {
					return nil, err
				}

				if len(option.Auth) > 0 {
					if _, err := c.Do("AUTH", option.Auth); err != nil {
						_ = c.Close()
						return nil, err
					}
				}

				if _, err := c.Do("SELECT", option.Db); err != nil {
					_ = c.Close()
					return nil, err
				}
				return c, nil
			},
		},
	}
}

func (p *Pool) HashCode() (hashValue uint32) {
	return crc32.ChecksumIEEE(p.id)
}

func (p *Pool) Get() (redis *Redis) {
	return &Redis{
		conn: p.pool.Get(),
	}
}

func (p *Pool) Put(redis *Redis) {
	_ = redis.Close()
}

type Redis struct {
	conn redigo.Conn
}

//释放连接到连接池，并非真正的close
func (r *Redis) Close() (err error) {
	return r.conn.Close()
}

func (r *Redis) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	return r.conn.Do(cmd, args...)
}

func (r *Redis) Send(cmd string, args ...interface{}) (err error) {
	return r.conn.Send(cmd, args...)
}

//region 1.0 Key
func (r *Redis) Exists(key []byte) (exists bool, err error) {
	var val int
	val, err = redigo.Int(r.conn.Do("EXISTS", key))
	return val == boot.SUCCESS, err
}

func (r *Redis) Expire(key []byte, timeoutSecond int64) (ok bool, err error) {
	_, err = redigo.Int(r.conn.Do("EXPIRE", key, timeoutSecond))
	return err == nil, err
}

func (r *Redis) ExpireAt(key []byte, unixTimestamp int64) (ok bool, err error) {
	_, err = redigo.Int(r.conn.Do("EXPIREAT", key, unixTimestamp))
	return err == nil, err
}

func (r *Redis) Ttl(key []byte) (timeout int64, err error) {
	return redigo.Int64(r.conn.Do("TTL", key))
}

func (r *Redis) Persist(key []byte) (ok bool, err error) {
	_, err = redigo.Int(r.conn.Do("PERSIST", key))
	return err == nil, err
}

func (r *Redis) Type(key []byte) (tp string, err error) {
	return redigo.String(r.conn.Do("TYPE", key))
}

func (r *Redis) Scan(cursor int64, pattern string, count int64) (nextCursor int64, keys [][]byte, err error) {
	var reply interface{}
	reply, err = r.conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", count)
	if err != nil {
		return
	}

	if values, ok := reply.([]interface{}); ok && len(values) == 2 {
		nextCursor, _ = strconv.ParseInt(string(values[0].([]byte)), 10, 64)
		items := values[1].([]interface{})
		keys = make([][]byte, len(items), len(items))
		for index, _ := range items {
			keys[index] = items[index].([]byte)
		}
	}

	return
}

func (r *Redis) ScanStrings(cursor int64, pattern string, count int64) (nextCursor int64, keys []string, err error) {
	var reply interface{}
	reply, err = r.conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", count)
	if err != nil {
		return
	}

	if values, ok := reply.([]interface{}); ok && len(values) == 2 {
		nextCursor, _ = strconv.ParseInt(string(values[0].([]byte)), 10, 64)
		items := values[1].([]interface{})
		keys = make([]string, len(items), len(items))
		for index, _ := range items {
			keys[index] = string(items[index].([]byte))
		}
	}

	return
}

func (r *Redis) Del(keys ...interface{}) (successCount int64, err error) {
	return redigo.Int64(r.conn.Do("DEL", keys...))
}

//endregion

//region 1.1 String
func (r *Redis) Get(key []byte) (value []byte, err error) {
	return redigo.Bytes(r.conn.Do("GET", key))
}

func (r *Redis) GetString(key []byte) (value string, err error) {
	return redigo.String(r.conn.Do("GET", key))
}

func (r *Redis) GetInt64(key []byte) (value int64, err error) {
	return redigo.Int64(r.conn.Do("GET", key))
}

func (r *Redis) GetInt(key []byte) (value int, err error) {
	return redigo.Int(r.conn.Do("GET", key))
}

func (r *Redis) Set(key []byte, params ...interface{}) (ok bool, err error) {
	args := boot.AcquireArgs()
	args = append(args, key)
	args = append(args, params...)
	var receive string
	receive, err = redigo.String(r.conn.Do("SET", args...))
	boot.ReleaseArgs(args)
	return strings.ToUpper(receive) == boot.OK, err
}

func (r *Redis) SetTimeout(key []byte, value interface{}, timeoutSecond int64) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.conn.Do("SETEX", key, timeoutSecond, value))
	return strings.ToUpper(receive) == boot.OK, err
}

//endregion

//region 1.2 Hash
func (r *Redis) HSet(key []byte, field []byte, value interface{}) (exists bool, err error) {
	var val int
	val, err = redigo.Int(r.conn.Do("HSET", key, field, value))
	if err != nil {
		return false, err
	}

	return val == boot.SUCCESS, err
}

func (r *Redis) HGet(key []byte, field []byte) (value []byte, err error) {
	return redigo.Bytes(r.conn.Do("HGET", key, field))
}

func (r *Redis) HGetString(key []byte, field []byte) (value string, err error) {
	return redigo.String(r.conn.Do("HGET", key, field))
}

func (r *Redis) HGetInt(key []byte, field []byte) (value int, err error) {
	return redigo.Int(r.conn.Do("HGET", key, field))
}

func (r *Redis) HGetInt64(key []byte, field []byte) (value int64, err error) {
	return redigo.Int64(r.conn.Do("HGET", key, field))
}

func (r *Redis) HMSet(key []byte, fieldValues map[string]interface{}) (ok bool, err error) {
	args := make([]interface{}, len(fieldValues)*2+1, len(fieldValues)*2+1)
	start := 0
	args[start] = key
	for field, _ := range fieldValues {
		start++
		args[start] = field
		start++
		args[start] = fieldValues[field]
	}
	var receive string
	receive, err = redigo.String(r.conn.Do("HMSET", args...))
	return receive == boot.OK, err
}

func (r *Redis) HMGet(key []byte, fields []string) (values []string, err error) {
	args := make([]interface{}, len(fields)+1, len(fields)+1)
	start := 0
	args[start] = key
	for start = 1; start <= len(fields); start++ {
		args[start] = fields[start]
	}
	return redigo.Strings(r.conn.Do("HMGET", args...))
}

func (r *Redis) HMGetMap(key []byte, fields []string) (fieldValues map[string]string, err error) {
	args := make([]interface{}, len(fields)+1, len(fields)+1)
	start := 0
	args[start] = key
	for start = 1; start <= len(fields); start++ {
		args[start] = fields[start]
	}
	var values []string
	values, err = redigo.Strings(r.conn.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}

	fieldValues = make(map[string]string, len(fields))
	for index, _ := range values {
		fieldValues[fields[index]] = values[index]
	}

	return
}

//endregion

func (r *Redis) Multi() *Multi {
	return multiGet()
}
