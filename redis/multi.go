package redis

import "sync"

var (
	multiPool = &sync.Pool{
		New: func() interface{} {
			return &Multi{cmdList: make([]Cmd, 0, 8)}
		},
	}

	multiGet = func() *Multi {
		return multiPool.Get().(*Multi)
	}

	multiPut = func(multi *Multi) {
		multi.cmdList = multi.cmdList[:0]
		multiPool.Put(multi)
	}
)

type Multi struct {
	cmdList []Cmd
}

type Cmd struct {
	cmd  string
	key  []byte
	args []interface{}
}

func NewCmd(cmd string, args ...interface{}) Cmd {
	return Cmd{
		cmd:  cmd,
		args: args,
	}
}

func (m *Multi) Send(cmd string, args ...interface{}) *Multi {
	m.cmdList = append(m.cmdList, NewCmd(cmd, args...))
	return m
}

func (m *Multi) ExecMulti(redis *Redis) ([]interface{}, error) {
	err := redis.conn.Send("MULTI")
	if err != nil {
		return nil, err
	}

	for _, cmd := range m.cmdList {
		err := redis.conn.Send(cmd.cmd, cmd.args...)
		if err != nil {
			return nil, err
		}
	}

	reply, err := redis.conn.Do("EXEC")
	multiPut(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}

func (m *Multi) ExecPipeline(redis *Redis) ([]interface{}, error) {
	for _, cmd := range m.cmdList {
		_ = redis.conn.Send(cmd.cmd, cmd.args...)
	}
	reply, err := redis.conn.Do("")
	multiPut(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}
