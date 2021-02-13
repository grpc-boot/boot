package mysql

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/grpc-boot/boot"
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

func (p *Pool) Query(sqlStr string, args ...interface{}) (*sql.Rows, error) {
	return p.db.Query(sqlStr, args...)
}

func (p *Pool) Execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return p.db.Exec(sqlStr, args...)
}

func (p *Pool) Find(query *Query) (*sql.Rows, error) {
	sqlStr, args := buildQuery(query)
	defer func() {
		boot.ReleaseArgs(*args)
	}()
	return p.db.Query(sqlStr, *args...)
}

func (p *Pool) Insert(table string, columns *map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildInsert(table, columns)
	res, err := p.db.Exec(sqlStr, *args...)
	boot.ReleaseArgs(*args)
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

func (p *Pool) BatchInsert(table string, rows *[]map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildBatchInsert(table, rows)
	res, err := p.db.Exec(sqlStr, *args...)
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
	sqlStr, args := buildUpdateAll(table, &set, &where)
	res, err := p.db.Exec(sqlStr, *args...)
	boot.ReleaseArgs(*args)
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
	sqlStr, args := buildDeleteAll(table, &where)
	res, err := p.db.Exec(sqlStr, *args...)
	boot.ReleaseArgs(*args)
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
