package boot

import (
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
