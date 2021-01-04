package boot

import (
	"database/sql"
	"math/rand"
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

func NewMysqlGroup(groupOption MysqlGroupOption) *MysqlGroup {
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

func (mg *MysqlGroup) Query(sqlStr string, args []interface{}, useMaster bool) (*sql.Rows, error) {
	return mg.SelectPool(useMaster).Query(sqlStr, args)
}

func (mg *MysqlGroup) Execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return mg.SelectPool(true).Execute(sqlStr, args...)
}
