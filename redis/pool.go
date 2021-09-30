package redis

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/boot"
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
	boot.CanHash

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
					if _, err = c.Do("AUTH", option.Auth); err != nil {
						_ = c.Close()
						return nil, err
					}
				}

				if _, err = c.Do("SELECT", option.Db); err != nil {
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

func (r *Redis) Do(cmd string, key interface{}, params ...interface{}) (reply interface{}, err error) {
	var args = make([]interface{}, 0, len(params)+1)
	args = append(args, key)
	args = append(args, params...)
	return r.conn.Do(cmd, args...)
}

func (r *Redis) Send(cmd string, args ...interface{}) (err error) {
	return r.conn.Send(cmd, args...)
}

//region 1.0 Key

func (r *Redis) Exists(key interface{}) (res int, err error) {
	return redigo.Int(r.conn.Do("EXISTS", key))
}

func (r *Redis) Expire(key interface{}, timeoutSecond int64) (ok bool, err error) {
	var res int
	res, err = redigo.Int(r.conn.Do("EXPIRE", key, timeoutSecond))
	return res == 1, err
}

func (r *Redis) ExpireAt(key interface{}, unixTimestamp int64) (ok bool, err error) {
	var res int
	res, err = redigo.Int(r.conn.Do("EXPIREAT", key, unixTimestamp))
	return res == 1, err
}

func (r *Redis) Ttl(key interface{}) (timeout int64, err error) {
	return redigo.Int64(r.conn.Do("TTL", key))
}

func (r *Redis) Persist(key interface{}) (ok bool, err error) {
	var res int
	res, err = redigo.Int(r.conn.Do("PERSIST", key))
	return res == 1, err
}

func (r *Redis) Type(key interface{}) (tp string, err error) {
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
func (r *Redis) Get(key interface{}) (value interface{}, err error) {
	return r.conn.Do("GET", key)
}

func (r *Redis) GetBytes(key interface{}) (value []byte, err error) {
	return redigo.Bytes(r.conn.Do("GET", key))
}

func (r *Redis) GetRange(key interface{}, start int, end int) (val string, err error) {
	return redigo.String(r.conn.Do("GETRANGE", key, start, end))
}

func (r *Redis) GetString(key interface{}) (value string, err error) {
	return redigo.String(r.conn.Do("GET", key))
}

func (r *Redis) GetInt64(key interface{}) (value int64, err error) {
	return redigo.Int64(r.conn.Do("GET", key))
}

func (r *Redis) Set(key interface{}, params ...interface{}) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.Do("SET", key, params...))
	return strings.ToUpper(receive) == boot.Ok, err
}

func (r *Redis) SetEx(key interface{}, value interface{}, timeoutSecond int64) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.conn.Do("SETEX", key, timeoutSecond, value))
	return strings.ToUpper(receive) == boot.Ok, err
}

func (r *Redis) SetRange(key interface{}, offset int, value []byte) (length int, err error) {
	return redigo.Int(r.conn.Do("SETRANGE", key, offset, value))
}

func (r *Redis) Strlen(key interface{}) (length int, err error) {
	return redigo.Int(r.conn.Do("STRLEN", key))
}

//endregion

//region 1.2 Bit

func (r *Redis) BitCount(key interface{}) (count int, err error) {
	return redigo.Int(r.conn.Do("BITCOUNT", key))
}

func (r *Redis) GetBit(key interface{}, offset int) (val int, err error) {
	return redigo.Int(r.conn.Do("GETBIT", key, offset))
}

func (r *Redis) SetBit(key interface{}, offset int, value int) (ok bool, err error) {
	_, err = redigo.Int(r.conn.Do("SETBIT", key, offset, value))
	return err == nil, err
}

//endregion

//region 1.3 Hash

func (r *Redis) HSet(key interface{}, field string, value interface{}) (isNew int, err error) {
	return redigo.Int(r.conn.Do("HSET", key, field, value))
}

func (r *Redis) HSetNx(key interface{}, field string, value interface{}) (ok bool, err error) {
	var isNew int
	isNew, err = redigo.Int(r.conn.Do("HSETNX", key, field, value))
	return isNew == 1, err
}

func (r *Redis) HIncrBy(key interface{}, field string, value int64) (newValue int64, err error) {
	return redigo.Int64(r.conn.Do("HINCRBY", key, field, value))
}

func (r *Redis) HIncrByFloat(key interface{}, field string, value float64) (newValue float64, err error) {
	return redigo.Float64(r.conn.Do("HINCRBYFLOAT", key, field, value))
}

func (r *Redis) HExists(key interface{}, field string) (exists bool, err error) {
	var (
		ets int
	)
	ets, err = redigo.Int(r.conn.Do("HEXISTS", key, field))
	return ets == 1, err
}

func (r *Redis) HGet(key interface{}, field string) (value []byte, err error) {
	return redigo.Bytes(r.conn.Do("HGET", key, field))
}

func (r *Redis) HGetString(key interface{}, field string) (value string, err error) {
	return redigo.String(r.conn.Do("HGET", key, field))
}

func (r *Redis) HGetInt64(key interface{}, field string) (value int64, err error) {
	return redigo.Int64(r.conn.Do("HGET", key, field))
}

func (r *Redis) HMSet(key interface{}, args ...interface{}) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.Do("HMSET", key, args...))
	return strings.ToUpper(receive) == boot.Ok, err
}

func (r *Redis) HMSetByMap(key interface{}, fieldValues map[string]interface{}) (ok bool, err error) {
	var (
		args  = make([]interface{}, len(fieldValues)*2, len(fieldValues)*2)
		start = 0
	)

	for field, _ := range fieldValues {
		start++
		args[start] = field
		start++
		args[start] = fieldValues[field]
	}

	return r.HMSet(key, args...)
}

func (r *Redis) HMGet(key interface{}, args ...interface{}) (values []string, err error) {
	return redigo.Strings(r.Do("HMGET", key, args...))
}

func (r *Redis) HMGetByArray(key interface{}, fields []string) (values []string, err error) {
	var (
		args = make([]interface{}, len(fields), len(fields))
	)

	for start := 0; start <= len(fields); start++ {
		args[start] = fields[start-1]
	}
	return r.HMGet(key, args...)
}

func (r *Redis) HMGetMap(key interface{}, fields []string) (fieldValues map[string]string, err error) {
	var (
		values []string
	)

	values, err = r.HMGetByArray(key, fields)
	if err != nil {
		return
	}

	fieldValues = make(map[string]string, len(fields))
	for index, _ := range values {
		fieldValues[fields[index]] = values[index]
	}

	return
}

func (r *Redis) HGetAll(key interface{}) (fieldValues map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("HGETALL", key))
}

func (r *Redis) HDel(key interface{}, fields ...interface{}) (delCount int, err error) {
	return redigo.Int(r.Do("HDEL", key, fields...))
}

func (r *Redis) HKeys(key interface{}) (keyList []string, err error) {
	return redigo.Strings(r.conn.Do("HKEYS", key))
}

func (r *Redis) HLen(key interface{}) (length int, err error) {
	return redigo.Int(r.conn.Do("HLEN", key))
}

//endregion

//region 1.4 List

func (r *Redis) LLen(key interface{}) (length int, err error) {
	return redigo.Int(r.conn.Do("LLEN", key))
}

func (r *Redis) LRange(key interface{}, start int, end int) (items []string, err error) {
	return redigo.Strings(r.conn.Do("LRANGE", key, start, end))
}

func (r *Redis) LPush(key interface{}, items ...interface{}) (totalLength int, err error) {
	return redigo.Int(r.Do("LPUSH", key, items...))
}

func (r *Redis) RPush(key interface{}, items ...interface{}) (totalLength int, err error) {
	return redigo.Int(r.Do("RPUSH", key, items...))
}

func (r *Redis) LPop(key interface{}) (item interface{}, err error) {
	return r.conn.Do("LPOP", key)
}

func (r *Redis) RPop(key interface{}) (item interface{}, err error) {
	return r.conn.Do("RPOP", key)
}

func (r *Redis) LRem(key interface{}, count int, value interface{}) (remCount int, err error) {
	return redigo.Int(r.conn.Do("LREM", key, count, value))
}

func (r *Redis) RPopLPush(source, destination interface{}) (item interface{}, err error) {
	return r.conn.Do("RPOPLPUSH", source, destination)
}

func (r *Redis) LTrim(key interface{}, start int, end int) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.Do("LTRIM", key, start, end))
	return strings.ToUpper(receive) == boot.Ok, err
}

func (r *Redis) LSet(key interface{}, index int, value interface{}) (ok bool, err error) {
	var receive string
	receive, err = redigo.String(r.conn.Do("LSET", key, index, value))
	return strings.ToUpper(receive) == boot.Ok, err
}

func (r *Redis) LIndex(key interface{}, index int) (item interface{}, err error) {
	return r.conn.Do("LINDEX", key, index)
}

//endregion

//region 1.5 Set

func (r *Redis) SAdd(key interface{}, items ...interface{}) (newCount int, err error) {
	return redigo.Int(r.Do("SADD", key, items...))
}

func (r *Redis) SCard(key interface{}) (total int, err error) {
	return redigo.Int(r.conn.Do("SCARD", key))
}

func (r *Redis) SIsMember(key interface{}, item interface{}) (exists bool, err error) {
	var isM int
	isM, err = redigo.Int(r.conn.Do("SISMEMBER", key, item))
	return isM == 1, err
}

func (r *Redis) SMembers(key interface{}) (items []string, err error) {
	return redigo.Strings(r.conn.Do("SMEMBERS", key))
}

func (r *Redis) SPop(key interface{}) (item interface{}, err error) {
	return r.conn.Do("SPOP", key)
}

//endregion

//region 1.6 ZSet

func (r *Redis) ZAdd(key interface{}, args ...interface{}) (newCount int, err error) {
	return redigo.Int(r.Do("ZADD", key, args...))
}

func (r *Redis) ZCard(key interface{}) (total int, err error) {
	return redigo.Int(r.conn.Do("ZCARD", key))
}

func (r *Redis) ZCount(key interface{}, min, max int) (num int, err error) {
	return redigo.Int(r.conn.Do("ZCOUNT", key, min, max))
}

func (r *Redis) ZRange(key interface{}, start, stop int) (items []string, err error) {
	return redigo.Strings(r.conn.Do("ZRANGE", key, start, stop))
}

func (r *Redis) ZRangeWithScores(key interface{}, start, stop int) (items map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
}

func (r *Redis) ZRevRange(key interface{}, start, stop int) (items []string, err error) {
	return redigo.Strings(r.conn.Do("ZREVRANGE", key, start, stop))
}

func (r *Redis) ZRevRangeWithScores(key interface{}, start, stop int) (items map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("ZREVRANGE", key, start, stop, "WITHSCORES"))
}

func (r *Redis) ZRank(key interface{}, item interface{}) (rank int, err error) {
	return redigo.Int(r.conn.Do("ZRANK", key, item))
}

//endregion

func (r *Redis) Multi() *Multi {
	return multiGet()
}
