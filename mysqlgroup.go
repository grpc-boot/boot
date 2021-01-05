package boot

import (
	"bytes"
	"database/sql"
	"math/rand"
)

var (
	dialPrefix    = []byte("dial")
	timeoutSuffix = []byte("i/o timeout")
)

type MysqlGroupOption struct {
	Masters []MysqlOption `yaml:"masters" json:"masters"`
	Slaves  []MysqlOption `yaml:"slaves" json:"slaves"`
}

type MysqlGroup struct {
	Masters []*MysqlPool
	Slaves  []*MysqlPool

	masterLen int
	slaveLen  int
}

func ConnLost(errMsg []byte) bool {
	if bytes.HasPrefix(errMsg, dialPrefix) && bytes.HasSuffix(errMsg, timeoutSuffix) {
		return true
	}
	return false
}

func NewMysqlGroup(groupOption *MysqlGroupOption) *MysqlGroup {
	if len(groupOption.Slaves) == 0 {
		groupOption.Slaves = groupOption.Masters
	}

	group := &MysqlGroup{
		masterLen: len(groupOption.Masters),
		slaveLen:  len(groupOption.Slaves),
	}

	group.Masters = make([]*MysqlPool, 0, group.masterLen)
	group.Slaves = make([]*MysqlPool, 0, group.slaveLen)

	for index, _ := range groupOption.Masters {
		pool, err := NewMysqlPool(&groupOption.Masters[index])
		if err != nil {
			panic(err.Error())
		}

		group.Masters = append(group.Masters, pool)
	}

	for index, _ := range groupOption.Slaves {
		pool, err := NewMysqlPool(&groupOption.Slaves[index])
		if err != nil {
			panic(err.Error())
		}

		group.Slaves = append(group.Slaves, pool)
	}

	return group
}

func (mg *MysqlGroup) DownMaster(index int) {
	if index >= mg.masterLen {
		return
	}
}

func (mg *MysqlGroup) SelectPool(useMaster bool) *MysqlPool {
	if useMaster {
		if mg.masterLen == 1 {
			return mg.Masters[0]
		}

		return mg.Masters[rand.Intn(mg.masterLen)]
	}

	if mg.slaveLen == 1 {
		return mg.Slaves[0]
	}

	return mg.Slaves[rand.Intn(mg.slaveLen)]
}

func (mg *MysqlGroup) Insert(table string, columns map[string]interface{}) (*ExecResult, error) {
	result, err := mg.SelectPool(true).Insert(table, columns)
	if err != nil {
		if ConnLost([]byte(err.Error())) {

		}
	}
}

func (mg *MysqlGroup) BatchInsert(table string, rows []map[string]interface{}) (*ExecResult, error) {
	return mg.SelectPool(true).BatchInsert(table, rows)
}

func (mg *MysqlGroup) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (*ExecResult, error) {
	return mg.SelectPool(true).UpdateAll(table, set, where)
}

func (mg *MysqlGroup) DeleteAll(table string, where map[string]interface{}) (*ExecResult, error) {
	return mg.SelectPool(true).DeleteAll(table, where)
}

func (mg *MysqlGroup) All(query *Query, useMaster bool) (*sql.Rows, error) {
	return mg.SelectPool(useMaster).All(query)
}

func (mg *MysqlGroup) One(query *Query, useMaster bool) *sql.Row {
	return mg.SelectPool(useMaster).One(query)
}
