package boot

import (
	"fmt"
	"strings"
	"sync"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

var (
	//命令池
	cmdListPool = &sync.Pool{
		New: func() interface{} {
			return make(Multi, 0, 8)
		},
	}
)

type RedisOption struct {
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

type RedisPool struct {
	pool *redigo.Pool
}

func NewPool(option *RedisOption) *RedisPool {
	return &RedisPool{
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

func (rp *RedisPool) Get() *Redis {
	return &Redis{
		conn: rp.pool.Get(),
	}
}

func (rp *RedisPool) Put(redis *Redis) {
	_ = redis.Close()
}

type Redis struct {
	conn redigo.Conn
}

//释放连接到连接池，并非真正的close
func (r *Redis) Close() error {
	return r.conn.Close()
}

func (r *Redis) Do(cmd string, args ...interface{}) (interface{}, error) {
	return r.conn.Do(cmd, args...)
}

func (r *Redis) Send(cmd string, args ...interface{}) error {
	return r.conn.Send(cmd, args...)
}

func (r *Redis) Multi() Multi {
	return acquireMulti()
}

//----------------------------------String----------------------------------------------
func (r *Redis) Get(key []byte) ([]byte, error) {
	return redigo.Bytes(r.conn.Do("GET", key))
}

func (r *Redis) Set(key []byte, params ...interface{}) bool {
	args := AcquireArgs()
	args = append(args, key)
	args = append(args, params...)
	receive, _ := redigo.String(r.conn.Do("SET", args...))
	ReleaseArgs(args)
	return strings.ToUpper(receive) == "OK"
}

func (r *Redis) SetTimeout(key []byte, value interface{}, timeoutSecond int64) bool {
	receive, _ := redigo.String(r.conn.Do("SETEX", key, timeoutSecond, value))
	return strings.ToUpper(receive) == "OK"
}

//----------------------------------String----------------------------------------------

//----------------------------------Multi----------------------------------------------
type Multi []Cmd

type Cmd struct {
	cmd  string
	key  []byte
	args []interface{}
}

func acquireMulti() Multi {
	return cmdListPool.Get().(Multi)
}

func releaseMulti(cmdList Multi) {
	cmdList = cmdList[:0]
	cmdListPool.Put(cmdList)
}

func NewCmd(cmd string, args ...interface{}) Cmd {
	return Cmd{
		cmd:  cmd,
		args: args,
	}
}

func (m Multi) Send(cmd string, args ...interface{}) Multi {
	m = append(m, NewCmd(cmd, args...))
	return m
}

func (m Multi) ExecMulti(redis *Redis) ([]interface{}, error) {
	err := redis.conn.Send("MULTI")
	if err != nil {
		return nil, err
	}

	for _, cmd := range m {
		err := redis.conn.Send(cmd.cmd, cmd.args...)
		if err != nil {
			return nil, err
		}
	}

	reply, err := redis.conn.Do("EXEC")
	releaseMulti(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}

func (m Multi) ExecPipeline(redis *Redis) ([]interface{}, error) {
	for _, cmd := range m {
		_ = redis.conn.Send(cmd.cmd, cmd.args...)
	}
	reply, err := redis.conn.Do("")
	releaseMulti(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}
