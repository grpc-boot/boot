package boot

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlOption struct {
	//格式："userName:password@schema(host:port)/dbName"，如：root:123456@tcp(127.0.0.1:3306)/test
	Dsn string `yaml:"dsn" json:"dsn"`
	//单位s
	MaxConnLifetime int `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxOpenConns    int `yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns    int `yaml:"maxIdleConns" json:"maxIdleConns"`
}

type MysqlPool struct {
	db *sql.DB
}

func NewMysqlPool(option *MysqlOption) (*MysqlPool, error) {
	db, err := sql.Open("mysql", option.Dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Duration(option.MaxConnLifetime) * time.Second)
	db.SetMaxIdleConns(option.MaxIdleConns)
	db.SetMaxOpenConns(option.MaxOpenConns)

	return &MysqlPool{
		db: db,
	}, nil
}

func (mp *MysqlPool) Db() *sql.DB {
	return mp.db
}

func (mp *MysqlPool) Query(sqlStr string, args ...interface{}) (*sql.Rows, error) {
	return mp.db.Query(sqlStr, args...)
}

func (mp *MysqlPool) Execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return mp.db.Exec(sqlStr, args...)
}

func (mp *MysqlPool) Find() *Query {
	return AcquireQuery()
}

func (mp *MysqlPool) All(query *Query) (*sql.Rows, error) {
	sqlStr, args := BuildQuery(query)
	defer func() {
		ReleaseQuery(query)
		ReleaseArgs(args)
	}()
	return mp.db.Query(sqlStr, args...)
}

func (mp *MysqlPool) One(query *Query) *sql.Row {
	query.limit = 1
	sqlStr, args := BuildQuery(query)
	defer func() {
		ReleaseQuery(query)
		ReleaseArgs(args)
	}()
	return mp.db.QueryRow(sqlStr, args...)
}

func (mp *MysqlPool) Insert(table string, columns map[string]interface{}) (sql.Result, error) {
	sqlBuffer := bytes.NewBufferString(fmt.Sprintf("INSERT INTO %s(", table))

	args := AcquireArgs()
	defer ReleaseArgs(args)

	values := make([]byte, 0, 7+2*len(columns))
	values = append(values, []byte("VALUES(")...)

	for field, arg := range columns {
		if len(values) > 7 {
			sqlBuffer.WriteByte(',')
		}
		sqlBuffer.Write([]byte(field))

		values = append(values, '?', ',')
		args = append(args, arg)
	}
	sqlBuffer.WriteByte(')')
	values[len(values)-1] = ')'
	sqlBuffer.Write(values)
	return mp.db.Exec(sqlBuffer.String(), args...)
}

func (mp *MysqlPool) BatchInsert(table string, rows []map[string]interface{}) (sql.Result, error) {
	fields := make([]string, 0, len(rows[0]))
	args := make([]interface{}, 0, len(rows)*len(rows[0]))
	values := make([]string, 0, len(rows))

	value := make([]byte, 0, 1+2*len(rows[0]))
	value = append(value, '(')
	for field, arg := range rows[0] {
		fields = append(fields, field)
		value = append(value, '?', ',')
		args = append(args, arg)
	}
	value[len(value)-1] = ')'

	values = append(values, string(value))

	for start := 1; start < len(rows); start++ {
		for _, arg := range rows[start] {
			args = append(args, arg)
		}
		values = append(values, string(value))
	}

	return mp.db.Exec(fmt.Sprintf("INSERT INTO %s(%s)VALUES%s", table, strings.Join(fields, ","), strings.Join(values, ",")), args...)
}

func (mp *MysqlPool) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (sql.Result, error) {
	sqlBuffer := bytes.NewBufferString(fmt.Sprintf("UPDATE %s SET ", table))

	args := AcquireArgs()
	defer ReleaseArgs(args)

	var num = 0
	for field, arg := range set {
		if num > 0 {
			sqlBuffer.WriteByte(',')
		} else {
			num++
		}
		sqlBuffer.Write([]byte(field))
		sqlBuffer.Write([]byte("=?"))
		args = append(args, arg)
	}

	if len(where) > 0 {
		condition, params := buildWhere(where)
		defer ReleaseArgs(params)

		sqlBuffer.Write(condition)
		args = append(args, params...)
	}

	return mp.db.Exec(sqlBuffer.String(), args...)
}

func (mp *MysqlPool) DeleteAll(table string, where map[string]interface{}) (sql.Result, error) {
	sqlBuffer := bytes.NewBufferString("DELETE FROM ")
	sqlBuffer.Write([]byte(table))

	if len(where) > 0 {
		condition, args := buildWhere(where)
		defer ReleaseArgs(args)

		sqlBuffer.Write(condition)
		return mp.db.Exec(sqlBuffer.String(), args...)
	}
	return mp.db.Exec(sqlBuffer.String())
}
