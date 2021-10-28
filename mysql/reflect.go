package mysql

import (
	"bytes"
	"errors"
	"reflect"
	"strings"

	"github.com/grpc-boot/boot"
)

const (
	tagName  = `bdb`
	required = `required`
	primary  = `primary`
)

var (
	ErrInvalidRowsTypes     = errors.New(`only struct and map[string]interface {} types are supported`)
	ErrNotFoundField        = errors.New(`failed to match the field from the struct to the database. Please configure the bdb tag correctly`)
	ErrNotFoundPrimaryField = errors.New(`failed to found primary field. Please configure the primary on bdb tag correctly`)
)

func BuildDeleteByReflect(table string, obj interface{}) (sqlStr string, args []interface{}, err error) {
	var (
		value     = reflect.ValueOf(obj)
		sqlBuffer = bytes.NewBuffer(nil)
	)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		return
	}

	var (
		t          = value.Type()
		fieldCount = value.NumField()

		dbField   string
		isPrimary bool
		where     = make(map[string]interface{}, 2)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isPrimary = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = "`" + tags[0] + "`"
		for _, val := range tags {
			if strings.TrimSpace(val) == primary {
				isPrimary = true
			}
		}

		if isPrimary {
			where[dbField] = value.Field(i).Interface()
			continue
		}
	}

	//没有找到主键
	if len(where) < 1 {
		return "", nil, ErrNotFoundPrimaryField
	}

	whereBytes, a := buildWhere(where)
	sqlBuffer.WriteString("DELETE FROM ")
	sqlBuffer.WriteByte('`')
	sqlBuffer.WriteString(table)
	sqlBuffer.WriteByte('`')
	sqlBuffer.Write(whereBytes)

	return sqlBuffer.String(), a, nil
}

func BuildUpdateByReflect(table string, obj interface{}) (sqlStr string, args []interface{}, err error) {
	var (
		value     = reflect.ValueOf(obj)
		sqlBuffer = bytes.NewBuffer(nil)
	)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		return
	}

	args = boot.AcquireArgs()

	var (
		t          = value.Type()
		fieldCount = value.NumField()

		dbField    string
		isRequired bool
		isPrimary  bool
		where      = make(map[string]interface{}, 2)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isRequired = false
		isPrimary = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = tags[0]
		for _, val := range tags {
			switch strings.TrimSpace(val) {
			case required:
				isRequired = true
			case primary:
				isPrimary = true
			}
		}

		if isPrimary {
			where[dbField] = value.Field(i).Interface()
			continue
		}

		if !isRequired && value.Field(i).IsZero() {
			continue
		}

		if len(args) > 0 {
			sqlBuffer.WriteByte(',')
		} else {
			sqlBuffer.WriteString("UPDATE ")
			sqlBuffer.WriteString(table)
			sqlBuffer.WriteString(" SET ")
		}

		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString(dbField)
		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString("=?")
		args = append(args, value.Field(i).Interface())
	}

	//没有找到主键
	if len(where) < 1 {
		return "", nil, ErrNotFoundPrimaryField
	}

	whereBytes, a := buildWhere(where)
	sqlBuffer.Write(whereBytes)
	args = append(args, a...)
	return sqlBuffer.String(), args, nil
}

func BuildInsertByReflect(table string, rows interface{}) (sql string, args []interface{}, err error) {
	var (
		vRows  = reflect.ValueOf(rows)
		values []reflect.Value
	)

	//指针类型转换
	if vRows.Kind() == reflect.Ptr {
		vRows = vRows.Elem()
	}

	switch vRows.Kind() {
	case reflect.Slice:
		values = make([]reflect.Value, 0, vRows.Len())
		for i := 0; i < vRows.Len(); i++ {
			values = append(values, vRows.Index(i))
		}
	case reflect.Struct:
		values = []reflect.Value{vRows}
	default:
		return "", nil, ErrInvalidRowsTypes
	}

	var (
		value      = values[0]
		sqlBuffer  = bytes.NewBuffer(nil)
		t          = value.Type()
		fieldCount = t.NumField()
	)

	if len(values) == 1 {
		args = boot.AcquireArgs()
	} else {
		args = make([]interface{}, 0, len(values)*fieldCount)
	}

	var (
		dbFieldList  = make([]string, 0, len(values))
		dbFieldCount int
		dbField      string
		isRequired   bool
		v            = make([]byte, 0, 2*fieldCount)
	)

	//寻找数据库字段和值
	for i := 0; i < fieldCount; i++ {
		dbField = t.Field(i).Tag.Get(tagName)
		isRequired = false

		if dbField == "" {
			continue
		}

		tags := strings.Split(dbField, ",")
		dbField = tags[0]
		for _, val := range tags {
			if val == required {
				isRequired = true
				break
			}
		}

		if !isRequired && value.Field(i).IsZero() {
			continue
		}

		dbFieldCount++
		dbFieldList = append(dbFieldList, t.Field(i).Name)

		if len(args) > 0 {
			v = append(v, ',')
			sqlBuffer.WriteByte(',')
		} else {
			v = append(v, '(')
			sqlBuffer.WriteString("INSERT INTO ")
			sqlBuffer.WriteString(table)
			sqlBuffer.WriteByte('(')
		}

		v = append(v, '?')
		sqlBuffer.WriteByte('`')
		sqlBuffer.WriteString(dbField)
		sqlBuffer.WriteByte('`')
		args = append(args, value.Field(i).Interface())
	}

	//没有找到字段
	if len(args) < 1 {
		return "", args, ErrNotFoundField
	}

	sqlBuffer.WriteByte(')')
	v = append(v, ')')

	sqlBuffer.WriteString("VALUES")
	sqlBuffer.Write(v)

	if len(values) > 1 {
		for start := 1; start < len(values); start++ {
			sqlBuffer.WriteByte(',')
			sqlBuffer.Write(v)
			for _, fieldName := range dbFieldList {
				args = append(args, values[start].FieldByName(fieldName).Interface())
			}
		}
	}

	return sqlBuffer.String(), args, nil
}
