package redis

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
