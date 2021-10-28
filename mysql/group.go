package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"net"
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/atomic"
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

	masterBadPool map[int]*atomic.Int64
	slaveBadPool  map[int]*atomic.Int64

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
		masterBadPool: make(map[int]*atomic.Int64, len(groupOption.Masters)),
		slaveBadPool:  make(map[int]*atomic.Int64, len(groupOption.Slaves)),
	}

	group.masters = make(map[int]*Pool, group.masterLen)
	group.slaves = make(map[int]*Pool, group.slaveLen)

	for index, _ := range groupOption.Masters {
		pool, err := NewPool(&groupOption.Masters[index])
		if err != nil {
			panic(err.Error())
		}

		group.masters[index] = pool
		group.masterBadPool[index] = &atomic.Int64{}
	}

	for index, _ := range groupOption.Slaves {
		pool, err := NewPool(&groupOption.Slaves[index])
		if err != nil {
			panic(err.Error())
		}

		group.slaves[index] = pool
		group.slaveBadPool[index] = &atomic.Int64{}
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

func (g *Group) up(index int, isMaster bool) {
	if isMaster {
		g.upMaster(index)
		return
	}
	g.upSlave(index)
}

func (g *Group) downMaster(index int) {
	if index >= g.masterLen {
		return
	}

	if g.masterBadPool[index].Get() > 0 {
		return
	}
	g.masterBadPool[index].Cas(0, time.Now().Unix())
}

func (g *Group) upMaster(index int) {
	if index >= g.masterLen {
		return
	}
	g.masterBadPool[index].Set(0)
}

func (g *Group) downSlave(index int) {
	if index >= g.slaveLen {
		return
	}

	if g.slaveBadPool[index].Get() > 0 {
		return
	}
	g.slaveBadPool[index].Cas(0, time.Now().Unix())
}

func (g *Group) upSlave(index int) {
	if index >= g.slaveLen {
		return
	}
	g.slaveBadPool[index].Set(0)
}

func (g *Group) GetBadPool(isMaster bool) (list []int) {
	if isMaster {
		list = make([]int, 0, g.masterLen)
		for index := 0; index < g.masterLen; index++ {
			if g.masterBadPool[index].Get() > 0 {
				list = append(list, index)
			}
		}
		return
	}

	list = make([]int, 0, g.slaveLen)
	for index := 0; index < g.slaveLen; index++ {
		if g.slaveBadPool[index].Get() > 0 {
			list = append(list, index)
		}
	}
	return
}

func (g *Group) GetMaster() (index int, mPoll *Pool, badTime int64) {
	if g.masterLen == 1 {
		return 0, g.masters[0], g.masterBadPool[0].Get()
	}

	current := time.Now().Unix()
	for index, mPoll = range g.masters {
		badTime = g.masterBadPool[index].Get()
		if badTime == 0 {
			return index, mPoll, badTime
		}

		if badTime+g.retryInterval < current {
			g.masterBadPool[index].Set(current)
			return index, mPoll, badTime
		}
	}

	return 0, g.masters[0], g.masterBadPool[0].Get()
}

func (g *Group) GetSlave() (index int, mPoll *Pool, badTime int64) {
	if g.slaveLen == 1 {
		return 0, g.slaves[0], g.slaveBadPool[0].Get()
	}

	current := time.Now().Unix()
	for index, mPoll = range g.slaves {
		badTime = g.slaveBadPool[index].Get()
		if badTime == 0 {
			return index, mPoll, badTime
		}

		if badTime+g.retryInterval < current {
			g.slaveBadPool[index].Set(current)
			return index, mPoll, badTime
		}
	}

	return 0, g.slaves[0], g.slaveBadPool[0].Get()
}

func (g *Group) SelectPool(isMaster bool) (index int, mPool *Pool, badTime int64) {
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

func (g *Group) isLostError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	}

	if errVal, ok := err.(*net.OpError); ok {
		log.Printf("exec sql error:%s", errVal.Error())
		return true
	}
	return false
}

func (g *Group) MasterExec(handler func(mPool *Pool) (interface{}, error)) (result interface{}, err error) {
	for start := 0; start < g.masterLen; start++ {
		index, pool, badTime := g.GetMaster()
		result, err = handler(pool)
		if err == nil {
			if badTime > 0 {
				g.upMaster(index)
			}

			return result, nil
		}

		if g.isLostError(err) {
			g.downMaster(index)
			continue
		}

		if badTime > 0 {
			g.upMaster(index)
		}

		return result, err
	}
	return nil, ErrNoMasterConn
}

func (g *Group) SlaveQuery(handler func(mPool *Pool) (interface{}, error)) (result interface{}, err error) {
	for start := 0; start < g.slaveLen; start++ {
		index, pool, badTime := g.GetSlave()
		result, err = handler(pool)
		if err == nil {
			if badTime > 0 {
				g.upSlave(index)
			}

			return result, err
		}

		if g.isLostError(err) {
			g.downSlave(index)
			continue
		}

		if badTime > 0 {
			g.upSlave(index)
		}
		return result, err
	}

	return nil, ErrNoSlaveConn
}

func (g *Group) Insert(table string, columns map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.Insert(table, columns)
	})

	return res.(*ExecResult), err
}

func (g *Group) BatchInsert(table string, rows []map[string]interface{}) (result *ExecResult, err error) {
	res, err := g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
		return mPool.BatchInsert(table, rows)
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
	var (
		result       interface{}
		sqlStr, args = buildQuery(query)
	)

	defer func() {
		boot.ReleaseArgs(&args)
	}()

	if useMaster {
		result, err = g.MasterExec(func(mPool *Pool) (i interface{}, e error) {
			return mPool.db.Query(sqlStr, args...)
		})

		return result.(*sql.Rows), err
	}

	result, err = g.SlaveQuery(func(mPool *Pool) (i interface{}, e error) {
		return mPool.db.Query(sqlStr, args...)
	})

	return result.(*sql.Rows), err
}
