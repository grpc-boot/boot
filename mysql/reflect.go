package mysql

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/grpc-boot/boot"
)

const (
	tagName  = `bdb`
	required = `required`
	primary  = `primary`
)

const (
	tableMethod = `TableName`
)

var (
	ErrInvalidRowsTypes     = errors.New(`only struct and map[string]interface {} types are supported`)
	ErrNotFoundField        = errors.New(`failed to match the field from the struct to the database. Please configure the bdb tag correctly`)
	ErrNotFoundPrimaryField = errors.New(`failed to found primary field. Please configure the primary on bdb tag correctly`)
	ErrInvalidTypes         = errors.New(`only *struct types are supported`)
	ErrInvalidFieldTypes    = errors.New(`only bool(1 is true, other is false),string、float64、float32、int、uint、int8、uint8、int16、uint16、int32、uint32、int64 and uint64 types are supported`)
)

func TableName(value reflect.Value) (tableName string) {
	v := value.MethodByName(tableMethod)
	if v.Kind() == reflect.Func {
		return v.Call(nil)[0].String()
	}
	return strings.ToLower(v.Type().Name())
}

func BuildInsertByObj(rows interface{}) (sql string, args []interface{}, err error) {
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
			sqlBuffer.WriteString(TableName(value))
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

func BuildDeleteByObj(obj interface{}) (sqlStr string, args []interface{}, err error) {
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
	sqlBuffer.WriteString(TableName(value))
	sqlBuffer.WriteByte('`')
	sqlBuffer.Write(whereBytes)

	return sqlBuffer.String(), a, nil
}

func BuildUpdateByObj(obj interface{}) (sqlStr string, args []interface{}, err error) {
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
			sqlBuffer.WriteString(TableName(value))
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

func Row2Obj(rows *sql.Rows, obj interface{}) error {
	fields, err := rows.Columns()
	if err != nil {
		return err
	}

	if len(fields) == 0 {
		return nil
	}

	values := make([]interface{}, len(fields), len(fields))
	for index, _ := range fields {
		values[index] = &[]byte{}
	}

	row := make(map[string][]byte, len(fields))
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		for index, field := range fields {
			row[field] = *values[index].(*[]byte)
		}
	}

	if len(row) < 1 {
		return nil
	}

	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Ptr {
		return ErrInvalidTypes
	}

	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return ErrInvalidTypes
	}

	var (
		fieldCount = v.NumField()
	)

	if fieldCount < 1 {
		return nil
	}

	var (
		t = v.Type()
	)

	for i := 0; i < fieldCount; i++ {
		tag := t.Field(i).Tag.Get(tagName)
		if tag == "" {
			continue
		}

		fieldName := strings.Split(tag, ",")[0]
		if _, exists := row[fieldName]; !exists {
			continue
		}

		switch v.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.Field(i).SetInt(boot.Bytes2Int64(row[fieldName]))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v.Field(i).SetUint(boot.Bytes2Uint64(row[fieldName]))
		case reflect.String:
			v.Field(i).SetString(string(row[fieldName]))
		case reflect.Float32, reflect.Float64:
			var val float64
			val, err = strconv.ParseFloat(string(row[fieldName]), 64)
			if err != nil {
				continue
			}
			v.Field(i).SetFloat(val)
		case reflect.Bool:
			v.Field(i).SetBool(string(row[fieldName]) == "1")
		default:
			return ErrInvalidFieldTypes
		}
	}

	return nil
}
