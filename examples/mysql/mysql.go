package main

import (
	"boot"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"
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
	//加载配置
	boot.Yaml("app.yml", config)

	//初始化mysqlGroup
	bootMysqlGroup = boot.NewMysqlGroup(&config.Boot)
}

func insert() {
	current := time.Now()
	result, err := bootMysqlGroup.Insert("`user`", map[string]interface{}{
		"`user_name`": strconv.FormatInt(current.UnixNano(), 10),
		"`add_time`":  current.Unix(),
	})

	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	fmt.Println("last insert id: ", result.LastInsertId)
}

func batchInsert() {
	result, err := bootMysqlGroup.BatchInsert("`user`", []map[string]interface{}{
		{
			"`user_name`": strconv.FormatInt(time.Now().UnixNano(), 10),
			"`add_time`":  time.Now().Unix(),
		},
		{
			"`user_name`": strconv.FormatInt(time.Now().UnixNano(), 10),
			"`add_time`":  time.Now().Unix(),
		},
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("batch insert affactedRows:", result.AffectedRows)
}

func update() {
	result, err := bootMysqlGroup.UpdateAll("`user`",
		map[string]interface{}{
			"`user_name`": "u" + strconv.FormatInt(time.Now().Unix(), 10),
		},
		map[string]interface{}{
			"`id`": 2,
		})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("update affactedRows:", result.AffectedRows)
}

func delete() {
	result, err := bootMysqlGroup.DeleteAll("`user`", map[string]interface{}{
		"id": 3,
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("delete affactedRows:", result.AffectedRows)
}

func equalQuery() {
	query := boot.AcquireQuery()

	rows, err := query.Select("id", "user_name", "add_time").
		From("user").
		Where(map[string]interface{}{
			"id": 1,
		}).
		Limit(0, 1).
		Query(bootMysqlGroup, false)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if rows.Next() {
		user := &User{}
		err := rows.Scan(&user.Id, &user.UserName, &user.AddTime)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		jsonBytes, _ := json.Marshal(user)
		fmt.Println("equal query:", string(jsonBytes))
	}
}

func inQuery() {
	query := boot.AcquireQuery()

	rows, err := query.From("`user`").
		Where(map[string]interface{}{
			"`id`": []interface{}{
				1, 2, 3,
			},
		}).Query(bootMysqlGroup, false)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	userList := make([]User, 0, 3)
	boot.FormatRows(rows, func(fieldValue map[string][]byte) {
		userList = append(userList, User{
			Id:       boot.Bytes2Int64(fieldValue["id"]),
			UserName: string(fieldValue["user_name"]),
			AddTime:  boot.Bytes2Int64(fieldValue["add_time"]),
		})
	})

	listBytes, _ := json.Marshal(userList)
	fmt.Println("in query:", string(listBytes))
}

func rangeQuery() {
	query := boot.AcquireQuery()
	rows, err := query.Select("`id`, `user_name`, `add_time`").
		From("`user`").
		Where(map[string]interface{}{
			"`add_time` <=": time.Now().Unix(),
			"`add_time` >":  1,
		}).
		Order("`add_time` DESC", "`id` DESC").
		Limit(0, 15).
		Query(bootMysqlGroup, false)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	userList, err := boot.ToMap(rows)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	listBytes, _ := json.Marshal(userList)
	fmt.Println("range query:", string(listBytes))
}

func likeQuery() {
	query := boot.AcquireQuery()
	rows, err := query.From("`user`").
		Where(map[string]interface{}{
			"`user_name` LIKE": "u%",
		}).
		Query(bootMysqlGroup, false)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	userList, err := boot.ToMap(rows)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	listBytes, _ := json.Marshal(userList)
	fmt.Println("like query:", string(listBytes))
}

func masterQuery() {
	query := boot.AcquireQuery()

	rows, err := query.Select("`id`", "`user_name`", "`add_time`").
		From("`user`").
		Where(map[string]interface{}{
			"`add_time` BETWEEN": []interface{}{
				0,
				time.Now().Unix(),
			},
		}).
		Order("`id` DESC").
		Limit(0, 1).
		Query(bootMysqlGroup, true)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if !rows.Next() {
		return
	}

	user := &User{}
	err = rows.Scan(&user.Id, &user.UserName, &user.AddTime)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	jsonBytes, _ := json.Marshal(user)
	fmt.Println("master query:", string(jsonBytes))
}

func main() {
	rand.Seed(time.Now().UnixNano())

	insert()
	batchInsert()
	update()
	delete()
	equalQuery()
	masterQuery()
	inQuery()
	rangeQuery()
	likeQuery()
}
