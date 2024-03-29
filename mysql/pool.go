package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/grpc-boot/boot"

	_ "github.com/go-sql-driver/mysql"
)

type PoolOption struct {
	//格式："userName:password@schema(host:port)/dbName"，如：root:123456@tcp(127.0.0.1:3306)/test
	Dsn string `yaml:"dsn" json:"dsn"`
	//单位s
	MaxConnLifetime int `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxOpenConns    int `yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns    int `yaml:"maxIdleConns" json:"maxIdleConns"`
}

type ExecResult struct {
	LastInsertId int64
	AffectedRows int64
}

type Pool struct {
	db *sql.DB
}

func NewPool(option *PoolOption) (*Pool, error) {
	db, err := sql.Open("mysql", option.Dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Duration(option.MaxConnLifetime) * time.Second)
	db.SetMaxIdleConns(option.MaxIdleConns)
	db.SetMaxOpenConns(option.MaxOpenConns)

	return &Pool{
		db: db,
	}, nil
}

func (p *Pool) Db() *sql.DB {
	return p.db
}

func (p *Pool) Query(sqlStr string, args ...interface{}) (rows *sql.Rows, err error) {
	return p.db.Query(sqlStr, args...)
}

func (p *Pool) Execute(sqlStr string, args ...interface{}) (result *ExecResult, err error) {
	res, err := p.db.Exec(sqlStr, args...)
	if err != nil {
		return
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	result.LastInsertId, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}
	return
}

func (p *Pool) Find(query *Query) (*sql.Rows, error) {
	sqlStr, args := buildQuery(query)
	defer func() {
		boot.ReleaseArgs(&args)
	}()
	return p.db.Query(sqlStr, args...)
}

func (p *Pool) Insert(table string, row map[string]interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args = buildInsertByMap(table, row)

	res, err := p.db.Exec(sqlStr, args...)
	boot.ReleaseArgs(&args)
	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	result.LastInsertId, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return
}

func (p *Pool) BatchInsert(table string, rows []map[string]interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args = buildInsertByMap(table, rows...)

	res, err := p.db.Exec(sqlStr, args...)
	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return
}

func (p *Pool) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildUpdateAll(table, set, where)
	res, err := p.db.Exec(sqlStr, args...)
	boot.ReleaseArgs(&args)
	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return
}

func (p *Pool) DeleteAll(table string, where map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildDeleteAll(table, where)
	res, err := p.db.Exec(sqlStr, args...)
	boot.ReleaseArgs(&args)
	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	return
}

func (p *Pool) Begin() (trans *Transaction, err error) {
	tx, err := p.db.Begin()
	if err != nil {
		return nil, err
	}

	return newTx(tx), err
}

func (p *Pool) BeginTx(ctx context.Context, opts *sql.TxOptions) (trans *Transaction, err error) {
	tx, err := p.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return newTx(tx), err
}

func (p *Pool) InsertObj(obj interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args, err = BuildInsertByObj(obj)
	if err != nil {
		return
	}

	res, err := p.db.Exec(sqlStr, args...)
	boot.ReleaseArgs(&args)
	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}

	result.LastInsertId, err = res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return
}

func (p *Pool) DeleteObj(obj interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args, err = BuildDeleteByObj(obj)
	if err != nil {
		return nil, err
	}

	res, err := p.db.Exec(sqlStr, args...)

	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}
	return
}

func (p *Pool) UpdateObj(obj interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args, err = BuildUpdateByObj(obj)
	if err != nil {
		return nil, err
	}

	res, err := p.db.Exec(sqlStr, args...)

	if err != nil {
		return nil, err
	}

	result = &ExecResult{}
	result.AffectedRows, err = res.RowsAffected()
	if err != nil {
		return nil, err
	}
	return
}
