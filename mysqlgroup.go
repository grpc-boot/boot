package boot

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync"
	"time"
)

var (
	ErrNoMasterConn = errors.New("mysql group: no master connection available")
	ErrNoSlaveConn  = errors.New("mysql group: no slave connection available")
)

type MysqlGroupOption struct {
	Masters []MysqlOption `yaml:"masters" json:"masters"`
	Slaves  []MysqlOption `yaml:"slaves" json:"slaves"`
	//单位s
	RetryInterval int64 `yaml:"retryInterval" json:"retryInterval"`
}

type MysqlGroup struct {
	masters map[int]*MysqlPool
	slaves  map[int]*MysqlPool

	mastersLock sync.Mutex
	slavesLock  sync.Mutex

	masterBadPool map[int]int64
	slaveBadPool  map[int]int64

	retryInterval int64
	masterLen     int
	slaveLen      int
}

func NewMysqlGroup(groupOption *MysqlGroupOption) *MysqlGroup {
	if len(groupOption.Slaves) == 0 {
		groupOption.Slaves = groupOption.Masters
	}

	group := &MysqlGroup{
		masterLen:     len(groupOption.Masters),
		slaveLen:      len(groupOption.Slaves),
		retryInterval: groupOption.RetryInterval,
		masterBadPool: make(map[int]int64, len(groupOption.Masters)),
		slaveBadPool:  make(map[int]int64, len(groupOption.Slaves)),
	}

	group.masters = make(map[int]*MysqlPool, group.masterLen)
	group.slaves = make(map[int]*MysqlPool, group.slaveLen)

	for index, _ := range groupOption.Masters {
		pool, err := NewMysqlPool(&groupOption.Masters[index])
		if err != nil {
			panic(err.Error())
		}

		group.masters[index] = pool
	}

	for index, _ := range groupOption.Slaves {
		pool, err := NewMysqlPool(&groupOption.Slaves[index])
		if err != nil {
			panic(err.Error())
		}

		group.slaves[index] = pool
	}

	return group
}

func (mg *MysqlGroup) down(index int, isMaster bool) {
	if isMaster {
		mg.downMaster(index)
		return
	}
	mg.downSlave(index)
}

func (mg *MysqlGroup) downMaster(index int) {
	if index >= mg.masterLen {
		return
	}

	mg.mastersLock.Lock()
	defer mg.mastersLock.Unlock()
	if _, ok := mg.masterBadPool[index]; ok {
		return
	}
	mg.masterBadPool[index] = time.Now().Unix()
}

func (mg *MysqlGroup) downSlave(index int) {
	if index >= mg.slaveLen {
		return
	}

	mg.slavesLock.Lock()
	defer mg.slavesLock.Unlock()

	if _, ok := mg.slaveBadPool[index]; ok {
		return
	}

	mg.slaveBadPool[index] = time.Now().Unix()
}

func (mg *MysqlGroup) GetMaster() (index int, mPoll *MysqlPool) {
	if mg.masterLen == 1 {
		return 0, mg.masters[0]
	}

	mg.mastersLock.Lock()
	defer mg.mastersLock.Unlock()

	for index, mPoll := range mg.masters {
		badTime, ok := mg.masterBadPool[index]
		if !ok {
			return index, mPoll
		}

		if badTime+mg.retryInterval < time.Now().Unix() {
			delete(mg.masterBadPool, index)
			return index, mPoll
		}
	}

	return 0, mg.masters[0]
}

func (mg *MysqlGroup) GetSlave() (index int, mPoll *MysqlPool) {
	if mg.slaveLen == 1 {
		return 0, mg.slaves[0]
	}

	mg.slavesLock.Lock()
	defer mg.slavesLock.Unlock()

	for index, mPoll := range mg.slaves {
		badTime, ok := mg.slaveBadPool[index]
		if !ok {
			return index, mPoll
		}

		if badTime+mg.retryInterval < time.Now().Unix() {
			delete(mg.slaveBadPool, index)
			return index, mPoll
		}
	}

	return 0, mg.slaves[0]
}

func (mg *MysqlGroup) SelectPool(isMaster bool) (index int, mPool *MysqlPool) {
	if isMaster {
		return mg.GetMaster()
	}
	return mg.GetSlave()
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

func (mg *MysqlGroup) MasterExec(handler func(mPool *MysqlPool) (interface{}, error)) (interface{}, error) {
	for start := 0; start < mg.masterLen; start++ {
		index, pool := mg.GetMaster()
		result, err := handler(pool)
		if err == nil {
			return result, nil
		}

		if isLostError(&err) {
			mg.downMaster(index)
			continue
		}

		return result, err
	}
	return nil, ErrNoMasterConn
}

func (mg *MysqlGroup) SlaveQuery(handler func(mPool *MysqlPool) (interface{}, error)) (interface{}, error) {
	for start := 0; start < mg.slaveLen; start++ {
		index, pool := mg.GetSlave()
		result, err := handler(pool)
		if err == nil {
			return result, err
		}

		if isLostError(&err) {
			mg.downSlave(index)
			continue
		}

		return result, err
	}

	return nil, ErrNoSlaveConn
}

func (mg *MysqlGroup) Insert(table string, columns map[string]interface{}) (*ExecResult, error) {
	result, err := mg.MasterExec(func(mPool *MysqlPool) (i interface{}, e error) {
		return mPool.Insert(table, columns)
	})

	return result.(*ExecResult), err
}

func (mg *MysqlGroup) BatchInsert(table string, rows []map[string]interface{}) (*ExecResult, error) {
	result, err := mg.MasterExec(func(mPool *MysqlPool) (i interface{}, e error) {
		return mPool.BatchInsert(table, rows)
	})

	return result.(*ExecResult), err
}

func (mg *MysqlGroup) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (*ExecResult, error) {
	result, err := mg.MasterExec(func(mPool *MysqlPool) (i interface{}, e error) {
		return mPool.UpdateAll(table, set, where)
	})

	return result.(*ExecResult), err
}

func (mg *MysqlGroup) DeleteAll(table string, where map[string]interface{}) (*ExecResult, error) {
	result, err := mg.MasterExec(func(mPool *MysqlPool) (i interface{}, e error) {
		return mPool.DeleteAll(table, where)
	})

	return result.(*ExecResult), err
}

func (mg *MysqlGroup) All(query *Query, useMaster bool) (*sql.Rows, error) {
	sqlStr, args := BuildQuery(query)
	defer func() {
		ReleaseQuery(query)
		ReleaseArgs(*args)
	}()

	if useMaster {
		result, err := mg.MasterExec(func(mPool *MysqlPool) (i interface{}, e error) {
			return mPool.db.Query(sqlStr, *args...)
		})

		return result.(*sql.Rows), err
	}

	result, err := mg.SlaveQuery(func(mPool *MysqlPool) (i interface{}, e error) {
		return mPool.db.Query(sqlStr, *args...)
	})

	return result.(*sql.Rows), err
}
