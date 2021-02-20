package redis

import "sync"

var (
	multiPool = &sync.Pool{
		New: func() interface{} {
			return &Multi{cmdList: make([]command, 0, 8)}
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
	cmdList []command
}

type command struct {
	cmd  string
	key  []byte
	args []interface{}
}

func (m *Multi) Send(cmd string, args ...interface{}) (multi *Multi) {
	m.cmdList = append(m.cmdList, command{
		cmd:  cmd,
		args: args,
	})
	return m
}

func (m *Multi) ExecMulti(redis *Redis) (results []interface{}, err error) {
	err = redis.conn.Send("MULTI")
	if err != nil {
		return nil, err
	}

	for _, cmd := range m.cmdList {
		err := redis.conn.Send(cmd.cmd, cmd.args...)
		if err != nil {
			return nil, err
		}
	}

	var reply interface{}
	reply, err = redis.conn.Do("EXEC")
	multiPut(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}

func (m *Multi) ExecPipeline(redis *Redis) (results []interface{}, err error) {
	for _, cmd := range m.cmdList {
		_ = redis.conn.Send(cmd.cmd, cmd.args...)
	}
	var reply interface{}
	reply, err = redis.conn.Do("")
	multiPut(m)
	if err != nil {
		return nil, err
	}

	return reply.([]interface{}), nil
}
