package boot

import (
	"bytes"
	"database/sql"
	"strconv"
	"strings"
)

const (
	inHolder     = "?,"
	defaultLimit = 1000
)

var (
	defaultColumns = "*"
	wherePrefix    = []byte(" WHERE ")
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

func (q *Query) All(group *MysqlGroup, useMaster bool) (*sql.Rows, error) {
	return group.All(q, useMaster)
}

func (q *Query) One(group *MysqlGroup, useMaster bool) *sql.Row {
	return group.One(q, useMaster)
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

	args = AcquireArgs()
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

func BuildQuery(q *Query) (sql string, args []interface{}) {
	where := ""

	condition, args := buildWhere(q.where)
	if len(condition) > 0 {
		where = string(condition)
	}

	return "SELECT " + q.columns + " FROM " + q.table + where + q.group + q.having + q.order + " LIMIT " + strconv.FormatInt(q.offset, 10) + "," + strconv.FormatInt(q.limit, 10), args
}
