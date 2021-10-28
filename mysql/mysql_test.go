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
  `nickname` varchar(32) DEFAULT '' COMMENT '用户名',
  `created_at` int(10) unsigned DEFAULT '0' COMMENT '添加时间',
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
	Id        int64  `bdb:"id,primary"`
	NickName  string `bdb:"nickname"`
	CreatedAt int64  `bdb:"createAt,required"`
}

func (u User) TableName() string {
	return `user`
}

func init() {
	config = &Config{}
	//加载配置
	err := boot.Yaml("app.yml", config)
	if err != nil {
		panic(err)
	}

	//初始化mysqlGroup
	group = NewGroup(&config.Boot)
}

func TestBuildInsertByReflect(t *testing.T) {
	user := User{Id: 5}
	sql, args, err := BuildInsertByReflect(user.TableName(), user)
	t.Log(sql, args, err)

	user.NickName = time.Now().String()
	user.Id = 0
	sql, args, err = BuildInsertByReflect(user.TableName(), &user)
	t.Log(sql, args, err)

	sql, args, err = BuildInsertByReflect(user.TableName(), []User{{
		NickName:  time.Now().String(),
		CreatedAt: time.Now().Unix(),
	},
		{
			NickName:  time.Now().String(),
			CreatedAt: time.Now().Unix(),
		},
	})
	t.Log(sql, args, err)
}

func TestBuildDeleteByReflect(t *testing.T) {
	user := User{Id: 5}
	sql, args, err := BuildDeleteByReflect(user.TableName(), user)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal(sql, args, err)
}

func TestBuildUpdateByReflect(t *testing.T) {
	user := User{Id: 5}
	sql, args, err := BuildUpdateByReflect(user.TableName(), user)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sql, args, err)

	user.NickName = time.Now().String()
	sql, args, err = BuildUpdateByReflect(user.TableName(), &user)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sql, args, err)
}

func TestGroup_Insert(t *testing.T) {
	current := time.Now()
	result, err := group.Insert("`user`", map[string]interface{}{
		"`nickname`":   strconv.FormatInt(current.UnixNano(), 10),
		"`created_at`": current.Unix(),
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
			"`nickname`":   strconv.FormatInt(time.Now().UnixNano(), 10),
			"`created_at`": time.Now().Unix(),
		},
		{
			"`nickname`":   strconv.FormatInt(time.Now().UnixNano(), 10),
			"`created_at`": time.Now().Unix(),
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
			"`nickname`": "u" + strconv.FormatInt(time.Now().Unix(), 10),
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

	rows, err := query.Select("id", "nickname", "created_at").
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
		err := rows.Scan(&user.Id, &user.NickName, &user.CreatedAt)
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

	rows, err := query.Select("`id`", "`nickname`", "`created_at`").
		From("`user`").
		Where(map[string]interface{}{
			"`created_at` BETWEEN": []interface{}{
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
	err = rows.Scan(&user.Id, &user.NickName, &user.CreatedAt)
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
			Id:        boot.Bytes2Int64(fieldValue["id"]),
			NickName:  string(fieldValue["nickname"]),
			CreatedAt: boot.Bytes2Int64(fieldValue["created_at"]),
		})
	})
}

func TestQueryRange(t *testing.T) {
	query := AcquireQuery()
	defer ReleaseQuery(query)
	rows, err := query.Select("`id`, `nickname`, `created_at`").
		From("`user`").
		Where(map[string]interface{}{
			"`created_at` <=": time.Now().Unix(),
			"`created_at` >":  1,
		}).
		Order("`created_at` DESC", "`id` DESC").
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
			"`nickname` LIKE": "u%",
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
		"created_at": time.Now().Unix(),
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
	t.Fatal(group.GetBadPool(true), group.GetBadPool(false))
}
