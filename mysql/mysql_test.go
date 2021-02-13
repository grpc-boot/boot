package mysql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/grpc-boot/boot"
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
	config *Config
	group  *Group
)

type Config struct {
	Boot GroupOption `yaml:"boot" json:"boot"`
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
	group = NewGroup(&config.Boot)
}

func TestGroup_Insert(t *testing.T) {
	current := time.Now()
	result, err := group.Insert("`user`", map[string]interface{}{
		"`user_name`": strconv.FormatInt(current.UnixNano(), 10),
		"`add_time`":  current.Unix(),
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	if result.AffectedRows != 1 || result.LastInsertId < 1 {
		t.Fatalf("insert failed")
	}
}

func TestGroup_BatchInsert(t *testing.T) {
	result, err := group.BatchInsert("`user`", []map[string]interface{}{
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
		t.Fatal(err.Error())
	}

	if result.AffectedRows != 2 {
		t.Fatalf("want affectedRows 2, got %d", result.AffectedRows)
	}
}

func TestGroup_UpdateAll(t *testing.T) {
	result, err := group.UpdateAll("`user`",
		map[string]interface{}{
			"`user_name`": "u" + strconv.FormatInt(time.Now().Unix(), 10),
		},
		map[string]interface{}{
			"`id`": 2,
		})

	if err != nil {
		t.Fatal(err.Error())
	}

	if result.AffectedRows != 1 {
		t.Fatalf("group:UpdateAll affactedRows:%d", result.AffectedRows)
	}
}

func TestGroup_DeleteAll(t *testing.T) {
	result, err := group.DeleteAll("`user`", map[string]interface{}{
		"id": 3,
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	fmt.Println("group:DeleteAll affactedRows:", result.AffectedRows)
}

func TestGroup_SlaveQuery(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)

	rows, err := query.Select("id", "user_name", "add_time").
		From("user").
		Where(map[string]interface{}{
			"id": 1,
		}).
		Limit(0, 1).
		QueryByGroup(group, false)

	if err != nil {
		t.Fatal(err.Error())
	}

	if rows.Next() {
		user := &User{}
		err := rows.Scan(&user.Id, &user.UserName, &user.AddTime)
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = json.Marshal(user)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}

func TestGroup_MasterQuery(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)

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
		QueryByGroup(group, true)

	if err != nil {
		t.Fatal(err.Error())
	}

	if !rows.Next() {
		return
	}

	user := &User{}
	err = rows.Scan(&user.Id, &user.UserName, &user.AddTime)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestQueryIn(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)

	rows, err := query.From("`user`").
		Where(map[string]interface{}{
			"`id`": []interface{}{
				1, 2, 3,
			},
		}).QueryByGroup(group, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	userList := make([]User, 0, 3)
	FormatRows(rows, func(fieldValue map[string][]byte) {
		userList = append(userList, User{
			Id:       boot.Bytes2Int64(fieldValue["id"]),
			UserName: string(fieldValue["user_name"]),
			AddTime:  boot.Bytes2Int64(fieldValue["add_time"]),
		})
	})
}

func TestQueryRange(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)
	rows, err := query.Select("`id`, `user_name`, `add_time`").
		From("`user`").
		Where(map[string]interface{}{
			"`add_time` <=": time.Now().Unix(),
			"`add_time` >":  1,
		}).
		Order("`add_time` DESC", "`id` DESC").
		Limit(0, 15).
		QueryByGroup(group, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = ToMap(rows)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestQueryLike(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)
	rows, err := query.From("`user`").
		Where(map[string]interface{}{
			"`user_name` LIKE": "u%",
		}).
		QueryByGroup(group, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	userList, err := ToMap(rows)
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = json.Marshal(userList)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestGroupTransaction(t *testing.T) {
	trans, err := group.Begin()
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = trans.UpdateAll("`user`", map[string]interface{}{
		"add_time": time.Now().Unix(),
	}, map[string]interface{}{
		"id": 2,
	})

	if err != nil {
		_ = trans.Rollback()
		t.Fatal(err.Error())
	}

	query := AcquireQuery()
	defer ReleaseQuery(query)

	query.From("`user`").
		Where(map[string]interface{}{
			"id": []interface{}{
				2, 3,
			},
		})
	rows, err := trans.Find(query)
	if err != nil {
		_ = trans.Rollback()
		t.Fatal(err.Error())
	}

	_, err = ToMap(rows)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = trans.Commit()
	if err != nil {
		t.Fatal(err.Error())
	}
}
