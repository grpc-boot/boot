package main

import (
	"boot"
	"encoding/json"
	"fmt"
)

/********************测试表结构***********************
CREATE TABLE `user` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_name` varchar(32) DEFAULT '' COMMENT '用户名',
  `add_time` int(10) unsigned DEFAULT '0' COMMENT '添加时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=uft8;
*/

var (
	config         *Config
	bootMysqlGroup *boot.MysqlGroup
)

type Config struct {
	Boot boot.MysqlGroupOption `yaml:"boot" json:"boot"`
}

type User struct {
	Id       int64
	UserName string
	AddTime  int64
}

func init() {
	config = &Config{}
	boot.Yaml("app.yml", config)
	bootMysqlGroup = boot.NewMysqlGroup(&config.Boot)
}

//使用group查询
func useGroup() {
	rows, err := bootMysqlGroup.Query("SELECT * FROM `user` ORDER BY `id` DESC LIMIT 10", nil, false)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	list := make([]User, 0, 10)

	boot.FormatRows(rows, func(fieldValue map[string][]byte) {
		list = append(list, User{
			Id:       boot.Bytes2Int64(fieldValue["id"]),
			UserName: string(fieldValue["user_name"]),
			AddTime:  boot.Bytes2Int64(fieldValue["add_time"]),
		})
	})
	data, _ := json.Marshal(list)
	fmt.Println(string(data))
}

func useGroupMaster() {
	rows, err := bootMysqlGroup.Query("SELECT * FROM `user` WHERE `id`=?", []interface{}{1}, true)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	list, _ := boot.ToMap(rows)
	data, _ := json.Marshal(list)
	fmt.Println(string(data))
}

func main() {
	useGroup()
	useGroupMaster()

}
