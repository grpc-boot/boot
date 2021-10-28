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

func buildWhere(where map[string]interface{}) (condition []byte, arguments *[]interface{}) {
	if len(where) < 1 {
		return
	}

	buf := bytes.NewBuffer(wherePrefix)

	var (
		operator string
		position int
		inLength int
	)

	args := boot.AcquireArgs()
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
	return whereCon[:len(whereCon)-3], &args
}

func buildQuery(q *Query) (sql string, arguments *[]interface{}) {
	where := ""

	condition, args := buildWhere(q.where)
	if len(condition) > 0 {
		where = string(condition)
	}

	return "SELECT " + q.columns + " FROM " + q.table + where + q.group + q.having + q.order + " LIMIT " + strconv.FormatInt(q.offset, 10) + "," + strconv.FormatInt(q.limit, 10), args
}

func buildInsert(table string, columns map[string]interface{}) (sql string, arguments *[]interface{}) {
	sqlBuffer := bytes.NewBufferString(fmt.Sprintf("INSERT INTO %s(", table))
	args := boot.AcquireArgs()

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

	return sqlBuffer.String(), &args
}

func buildBatchInsert(table string, rows []map[string]interface{}) (sql string, arguments *[]interface{}) {
	fields := make([]string, 0, len((rows)[0]))
	args := make([]interface{}, 0, len(rows)*len((rows)[0]))
	values := make([]string, 0, len(rows))

	value := make([]byte, 0, 1+2*len((rows)[0]))
	value = append(value, '(')
	for field, arg := range (rows)[0] {
		fields = append(fields, field)
		value = append(value, '?', ',')
		args = append(args, arg)
	}
	value[len(value)-1] = ')'

	values = append(values, string(value))

	for start := 1; start < len(rows); start++ {
		for _, arg := range (rows)[start] {
			args = append(args, arg)
		}
		values = append(values, string(value))
	}

	return fmt.Sprintf("INSERT INTO %s(%s)VALUES%s", table, strings.Join(fields, ","), strings.Join(values, ",")), &args
}

func buildUpdateAll(table string, set map[string]interface{}, where map[string]interface{}) (sql string, arguments *[]interface{}) {
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
		args = append(args, *params...)
		boot.ReleaseArgs(*params)
	}

	return sqlBuffer.String(), &args
}

func buildDeleteAll(table string, where map[string]interface{}) (sql string, arguments *[]interface{}) {
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
