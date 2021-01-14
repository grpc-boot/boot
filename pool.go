package boot

import "sync"

var (
	//参数池
	argsPool = &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 8)
		},
	}
)

func AcquireArgs() []interface{} {
	return argsPool.Get().([]interface{})
}

func ReleaseArgs(args []interface{}) {
	if args == nil {
		return
	}

	args = args[:0]
	argsPool.Put(args)
}
