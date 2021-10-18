package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/grpc-boot/boot"
)

var (
	ErrNoMasterConn = errors.New("mysql group: no master connection available")
	ErrNoSlaveConn  = errors.New("mysql group: no slave connection available")
)

type GroupOption struct {
	Masters []PoolOption `yaml:"masters" json:"masters"`
	Slaves  []PoolOption `yaml:"slaves" json:"slaves"`
	//单位s
	RetryInterval int64 `yaml:"retryInterval" json:"retryInterval"`
}

type Group struct {
	masters map[int]*Pool
	slaves  map[int]*Pool

	masterBadPool map[int]*int64
	slaveBadPool  map[int]*int64

	retryInterval int64
	masterLen     int
	slaveLen      int
}

func NewGroup(groupOption *GroupOption) *Group {
	if len(groupOption.Slaves) == 0 {
		groupOption.Slaves = groupOption.Masters
	}

	group := &Group{
		masterLen:     len(groupOption.Masters),
		slaveLen:      len(groupOption.Slaves),
		retryInterval: groupOption.RetryInterval,
		masterBadPool: make(map[int]*int64, len(groupOption.Masters)),
		slaveBadPool:  make(map[int]*int64, len(groupOption.Slaves)),
	}

	group.masters = make(map[int]*Pool, group.masterLen)
	group.slaves = make(map[int]*Pool, group.slaveLen)

	for index, _ := range groupOption.Masters {
		pool, err := NewPool(&groupOption.Masters[index])
		if err != nil {
			panic(err.Error())
		}

		group.masters[index] = pool
		var badTime int64
		group.masterBadPool[index] = &badTime
	}

	for index, _ := range groupOption.Slaves {
		pool, err := NewPool(&groupOption.Slaves[index])
		if err != nil {
			panic(err.Error())
		}

		group.slaves[index] = pool
		var badTime int64
		group.slaveBadPool[index] = &badTime
	}

	return group
}

func (g *Group) down(index int, isMaster bool) {
	if isMaster {
		g.downMaster(index)
		return
	}
	g.downSlave(index)
}

func (g *Group) downMaster(index int) {
	if index >= g.masterLen {
		return
	}

	old := atomic.LoadInt64(g.masterBadPool[index])
	if old > 0 {
		return
	}
	atomic.CompareAndSwapInt64(g.masterBadPool[index], 0, time.Now().Unix())
}

func (g *Group) downSlave(index int) {
	if index >= g.slaveLen {
		return
	}

	old := atomic.LoadInt64(g.slaveBadPool[index])
	if old > 0 {
		return
	}
	atomic.CompareAndSwapInt64(g.slaveBadPool[index], 0, time.Now().Unix())
}

func (g *Group) GetBadPool(isMaster bool) (list []int) {
	if isMaster {
		list = make([]int, 0, g.masterLen)
		for index := 0; index < g.masterLen; index++ {
			badTime := atomic.LoadInt64(g.masterBadPool[index])
			if badTime > 0 {
				list = append(list, index)
			}
		}
		return
	}

	list = make([]int, 0, g.slaveLen)
	for index := 0; index < g.slaveLen; index++ {
		badTime := atomic.LoadInt64(g.slaveBadPool[index])
		if badTime > 0 {
			list = append(list, index)
		}
	}
	return
}

func (g *Group) GetMaster() (index int, mPoll *Pool) {
	if g.masterLen == 1 {
		return 0, g.masters[0]
	}

	for index, mPoll := range g.masters {
		badTime := atomic.LoadInt64(g.masterBadPool[index])
		if badTime == 0 {
			return index, mPoll
		}

		if badTime+g.retryInterval < time.Now().Unix() {
			atomic.StoreInt64(g.masterBadPool[index], 0)
			return index, mPoll
		}
	}

	return 0, g.masters[0]
}

func (g *Group) GetSlave() (index int, mPoll *Pool) {
	if g.slaveLen == 1 {
		return 0, g.slaves[0]
	}

	for index, mPoll := range g.slaves {
		badTime := atomic.LoadInt64(g.slaveBadPool[index])
		if badTime == 0 {
			return index, mPoll
		}

		if badTime+g.retryInterval < time.Now().Unix() {
			atomic.StoreInt64(g.slaveBadPool[index], 0)
			return index, mPoll
		}
	}

	return 0, g.slaves[0]
}

func (g *Group) SelectPool(isMaster bool) (index int, mPool *Pool) {
	if isMaster {
		return g.GetMaster()
	}
	return g.GetSlave()
}

func (g *Group) BeginTx(ctx context.Context, opts *sql.TxOptions) (trans *Transaction, err error) {
	tx, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.db.BeginTx(ctx, opts)
	})
	if err != nil {
		return nil, err
	}
	return newTx(tx.(*sql.Tx)), err
}

func (g *Group) Begin() (trans *Transaction, err error) {
	tx, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.db.Begin()
	})
	if err != nil {
		return nil, err
	}
	return newTx(tx.(*sql.Tx)), err
}

func isLostError(err *error) bool {
	if *err == driver.ErrBadConn {
		return true
	}

	if errVal, ok := (*err).(*net.OpError); ok {
		return errVal.Op == "dial" && errVal.Timeout()
	}

	return false
}

func (g *Group) MasterExec(handler func(mPool *Pool) (interface{}, error)) (result interface{}, err error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool := g.GetMaster()
		result, err = handler(pool)
		if err == nil {
			return result, nil
		}

		if isLostError(&err) {
			g.downMaster(index)
			continue
		}

		return result, err
	}
	return nil, ErrNoMasterConn
}

func (g *Group) SlaveQuery(handler func(mPool *Pool) (interface{}, error)) (result interface{}, err error) {
	for start := 0; start < g.slaveLen; start++ {
		index, pool := g.GetSlave()
		result, err = handler(pool)
		if err == nil {
			return result, err
		}

		if isLostError(&err) {
			g.downSlave(index)
			continue
		}

		return result, err
	}

	return nil, ErrNoSlaveConn
}

func (g *Group) Insert(table string, columns map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.Insert(table, &columns)
	})

	return res.(*ExecResult), err
}

func (g *Group) BatchInsert(table string, rows []map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.BatchInsert(table, &rows)
	})

	return res.(*ExecResult), err
}

func (g *Group) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.UpdateAll(table, set, where)
	})

	return res.(*ExecResult), err
}

func (g *Group) DeleteAll(table string, where map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.DeleteAll(table, where)
	})

	return res.(*ExecResult), err
}

func (g *Group) Find(query *Query, useMaster bool) (rows *sql.Rows, err error) {
	sqlStr, args := buildQuery(query)
	defer func() {
		boot.ReleaseArgs(*args)
	}()

	if useMaster {
		result, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
			return mPool.db.Query(sqlStr, *args...)
		})

		return result.(*sql.Rows), err
	}

	result, err := g.SlaveQuery(func(mPool *Pool) (i interface{}, e error) {
		return mPool.db.Query(sqlStr, *args...)
	})

	return result.(*sql.Rows), err
}
