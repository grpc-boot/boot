package mysql

import (
	"database/sql"
	"github.com/grpc-boot/boot"
)

type Transaction struct {
	tx *sql.Tx
}

func newTx(tx *sql.Tx) *Transaction {
	return &Transaction{
		tx: tx,
	}
}

func (t *Transaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

func (t *Transaction) Query(sqlStr string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.Query(sqlStr, args...)
}

func (t *Transaction) Execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return t.tx.Exec(sqlStr, args...)
}

func (t *Transaction) Find(query *Query) (*sql.Rows, error) {
	sqlStr, args := buildQuery(query)
	defer func() {
		ReleaseQuery(query)
		boot.ReleaseArgs(&args)
	}()
	return t.tx.Query(sqlStr, args...)
}

func (t *Transaction) Insert(table string, row interface{}) (result *ExecResult, err error) {
	var (
		sqlStr string
		args   []interface{}
	)

	sqlStr, args, err = buildInsert(table, row)
	if err != nil {
		return nil, err
	}

	res, err := t.tx.Exec(sqlStr, args...)
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

func (t *Transaction) BatchInsert(table string, rows interface{}) (result *ExecResult, err error) {
	var (
		_, ok  = rows.([]map[string]interface{})
		sqlStr string
		args   []interface{}
	)

	if ok {
		sqlStr, args = buildInsertByMap(table, rows.([]map[string]interface{})...)
	} else {
		sqlStr, args, err = BuildInsertByObj(table, rows)
		if err != nil {
			return nil, err
		}
	}
	res, err := t.tx.Exec(sqlStr, args...)
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

func (t *Transaction) UpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildUpdateAll(table, set, where)
	res, err := t.tx.Exec(sqlStr, args...)
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

func (t *Transaction) DeleteAll(table string, where map[string]interface{}) (result *ExecResult, err error) {
	sqlStr, args := buildDeleteAll(table, where)
	res, err := t.tx.Exec(sqlStr, args...)
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
