package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/grpc-boot/boot"
)

const (
	inHolder     = "?,"
	defaultLimit = 1000
)

var (
	defaultColumns = "*"
	wherePrefix    = []byte(" WHERE ")
	queryPool      = &sync.Pool{
		New: func() interface{} {
			query := &Query{
				limit:   defaultLimit,
				columns: defaultColumns,
			}
			return query
		},
	}

	AcquireQuery = func() *Query {
		return queryPool.Get().(*Query)
	}

	ReleaseQuery = func(query *Query) {
		query = query.reset()
		queryPool.Put(query)
	}
)

type Query struct {
	table   string
	columns string
	where   map[string]interface{}
	group   string
	having  string
	order   string
	offset  int64
	limit   int64
}

//---------------------查询对象池--------------------------

func (q *Query) reset() *Query {
	q.table = ""
	q.columns = defaultColumns
	q.offset = 0
	q.limit = defaultLimit
	q.where = nil
	q.group = ""
	q.having = ""
	q.order = ""

	return q
}

func (q *Query) From(table string) *Query {
	q.table = table
	return q
}

func (q *Query) Select(columns ...string) *Query {
	q.columns = strings.Join(columns, ",")
	return q
}

func (q *Query) Where(where map[string]interface{}) *Query {
	q.where = where
	return q
}

func (q *Query) Group(fields ...string) *Query {
	q.group = " GROUP BY " + strings.Join(fields, ",")
	return q
}

func (q *Query) Having(having string) *Query {
	q.having = " HAVING " + having
	return q
}

func (q *Query) Order(orders ...string) *Query {
	q.order = " ORDER BY " + strings.Join(orders, ",")
	return q
}

func (q *Query) Offset(offset int64) *Query {
	q.offset = offset
	return q
}

func (q *Query) Limit(offset int64, limit int64) *Query {
	if limit == 0 {
		limit = defaultLimit
	}
	q.limit = limit
	q.offset = offset
	return q
}

func (q *Query) Query(pool *Pool) (*sql.Rows, error) {
	return pool.Find(q)
}

func (q *Query) QueryByGroup(group *Group, useMaster bool) (*sql.Rows, error) {
	return group.Find(q, useMaster)
}

func buildWhere(where map[string]interface{}) (condition []byte, args []interface{}) {
	if len(where) < 1 {
		return
	}

	buf := bytes.NewBuffer(wherePrefix)

	var (
		operator string
		position int
		inLength int
	)

	args = make([]interface{}, 0)
	for field, value := range where {
		operator = "="
		position = strings.Index(field, " ")
		if position > 0 {
			operator = strings.ToUpper(field[position+1:])
			field = field[:position]
		}

		if val, ok := value.([]interface{}); ok {
			inLength = len(val)
			if operator != "BETWEEN" {
				operator = "IN"
			}
			args = append(args, val...)
		} else {
			args = append(args, value)
		}

		buf.WriteString(" (")
		buf.WriteString(field)

		switch operator {
		case "IN":
			buf.WriteString(" IN(")
			buf.WriteString(strings.Repeat(inHolder, inLength)[:2*inLength-1])
			buf.WriteString(")) AND")
		case "BETWEEN":
			buf.WriteString(" BETWEEN ? AND ?) AND")
		case "LIKE":
			buf.WriteString(" LIKE ?) AND")
		default:
			buf.WriteString(" ")
			buf.WriteString(operator)
			buf.WriteString(" ?) AND")
		}
	}

	whereCon := buf.Bytes()
	return whereCon[:len(whereCon)-3], args
}

func buildQuery(q *Query) (sql string, arguments []interface{}) {
	where := ""

	condition, args := buildWhere(q.where)
	if len(condition) > 0 {
		where = string(condition)
	}

	return "SELECT " + q.columns + " FROM " + q.table + where + q.group + q.having + q.order + " LIMIT " + strconv.FormatInt(q.offset, 10) + "," + strconv.FormatInt(q.limit, 10), args
}

func buildInsert(table string, row interface{}) (sql string, arguments []interface{}, err error) {
	var (
		sqlStr      string
		args        []interface{}
		columns, ok = row.(map[string]interface{})
	)

	if ok {
		sqlStr, args = buildInsertByMap(table, columns)
	} else {
		sqlStr, args, err = BuildInsertByObj(table, row)
		if err != nil {
			return "", nil, err
		}
	}

	return sqlStr, args, nil
}

func buildInsertByMap(table string, rows ...map[string]interface{}) (sql string, arguments []interface{}) {
	if len(rows) < 0 {
		return "", nil
	}

	var (
		row       = rows[0]
		sqlBuffer = bytes.NewBuffer(nil)
		args      []interface{}
	)

	if len(rows) == 1 {
		args = boot.AcquireArgs()
	} else {
		args = make([]interface{}, 0, len(rows)*len(row))
	}

	var (
		dbFieldList = make([]string, 0, len(rows))
		v           = make([]byte, 0, 2*len(rows))
	)

	for field, value := range row {
		if len(args) > 0 {
			v = append(v, ',')
			sqlBuffer.WriteByte(',')
		} else {
			v = append(v, '(')
			sqlBuffer.WriteString("INSERT INTO ")
			sqlBuffer.WriteString(table)
			sqlBuffer.WriteByte('(')
		}

		dbFieldList = append(dbFieldList, field)
		v = append(v, '?')
		sqlBuffer.WriteString(field)
		args = append(args, value)
	}

	//没有找到字段
	if len(args) < 1 {
		return "", args
	}

	sqlBuffer.WriteByte(')')
	v = append(v, ')')

	sqlBuffer.WriteString("VALUES")
	sqlBuffer.Write(v)

	if len(rows) > 1 {
		for start := 1; start < len(rows); start++ {
			sqlBuffer.WriteByte(',')
			sqlBuffer.Write(v)
			for _, field := range dbFieldList {
				args = append(args, rows[start][field])
			}
		}
	}

	return sqlBuffer.String(), args
}

func buildUpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (sql string, arguments []interface{}) {
	sqlBuffer := bytes.NewBufferString(fmt.Sprintf("UPDATE %s SET ", table))
	args := boot.AcquireArgs()
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
		sqlBuffer.Write(condition)
		args = append(args, params...)
	}

	return sqlBuffer.String(), args
}

func buildDeleteAll(table string, where map[string]interface{}) (sql string, arguments []interface{}) {
	sqlBuffer := bytes.NewBufferString("DELETE FROM ")
	sqlBuffer.Write([]byte(table))

	if len(where) > 0 {
		condition, args := buildWhere(where)

		sqlBuffer.Write(condition)
		return sqlBuffer.String(), args
	}
	return sqlBuffer.String(), nil
}

type RowFormat func(fieldValue map[string][]byte)

func FormatRows(rows *sql.Rows, handler RowFormat) {
	fields, err := rows.Columns()
	if err != nil {
		return
	}

	if len(fields) == 0 {
		return
	}

	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return
		}

		row := make(map[string][]byte, len(fields))
		for index, field := range fields {
			row[field] = *values[index].(*[]byte)
		}

		handler(row)
	}
}

func ToMap(rows *sql.Rows) ([]map[string]string, error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		return nil, nil
	}

	var data []map[string]string
	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]string, len(fields))
		for index, field := range fields {
			row[field] = string(*values[index].(*[]byte))
		}
		data = append(data, row)
	}

	return data, nil
}
