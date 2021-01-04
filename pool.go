package boot

import "sync"

var (
	//参数池
	argsPool = &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, 0, 8)
		},
	}

	queryPool = &sync.Pool{
		New: func() interface{} {
			query := &Query{
				limit:   defaultLimit,
				columns: defaultColumns,
			}
			return query
		},
	}
)

func AcquireArgs() []interface{} {
	return argsPool.Get().([]interface{})
}

func ReleaseArgs(args []interface{}) {
	args = args[:0]
	argsPool.Put(args)
}

func AcquireQuery() *Query {
	return queryPool.Get().(*Query)
}

func ReleaseQuery(query *Query) {
	query = query.reset()
	queryPool.Put(query)
}
