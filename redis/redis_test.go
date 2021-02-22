package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/grpc-boot/boot"
)

var (
	group  *Group
	config *Config
)

type Config struct {
	Boot []Option `yaml:"boot" json:"boot"`
}

func init() {
	config = &Config{}
	//加载配置
	boot.Yaml("app.yml", config)

	//初始化redisGroup
	group = NewGroup(config.Boot, nil)
}

func TestGroup_Get(t *testing.T) {
	key := []byte("user:12345")

	pool, err := group.Get(key)
	if err != nil {
		t.Fatal(err.Error())
	}

	r := pool.Get()
	defer pool.Put(r)

	result, _ := r.Set(key, 15)
	if !result {
		t.Fatal("want true, got false")
	}

	result, _ = r.Expire(key, 30)
	if !result {
		t.Fatal("want true, got false")
	}

	timeout, _ := r.Ttl(key)
	if timeout != 30 {
		t.Fatalf("want 30, got %d", timeout)
	}

	result, _ = r.Persist(key)
	if !result {
		t.Fatal("want true, got false")
	}

	te, _ := r.Type(key)
	if te != "string" {
		t.Fatalf("want string, got %s", te)
	}

	var val int
	val, err = r.GetInt(key)
	if val != 15 {
		t.Fatal("want true, got false")
	}

	result, _ = r.ExpireAt(key, time.Now().Unix())
	if !result {
		t.Fatal("want true, got false")
	}

	val, err = r.GetInt(key)
	if val == 15 {
		t.Fatal("want false, got true")
	}

	ok, err := r.Set([]byte("age:12345"), 14)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !ok {
		t.Fatal("want true, got false")
	}

	v, err := r.GetInt64([]byte("age:12345"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	if v != 14 {
		t.Fatal("want true, got false")
	}
}

func TestRedis_ScanStrings(t *testing.T) {
	pool, err := group.Index(0)
	if err != nil {
		t.Fatal(err.Error())
	}

	r := pool.Get()
	defer pool.Put(r)

	cursor, keys, err := r.ScanStrings(0, "*", 1000)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) > 0 {
		fmt.Println(keys)
	}

	for cursor != 0 {
		cursor, keys, err = r.ScanStrings(cursor, "*", 100)
	}

}

func TestRedis_SetRange(t *testing.T) {
	key := []byte("bit:12345")
	pool, err := group.Index(0)
	if err != nil {
		t.Fatal(err.Error())
	}

	r := pool.Get()
	defer pool.Put(r)

	count, err := r.SetRange(key, 1024, []byte{byte(0)})
	if err != nil {
		t.Fatal(err)
	}

	length, err := r.Strlen(key)
	if err != nil {
		t.Fatal(err)
	}

	if count != length {
		t.Fatalf("want true, got false")
	}

	ok, err := r.SetBit(key, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("want true, got false")
	}
	val, err := r.GetBit(key, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Fatalf("want 1, got %d", val)
	}

	val, err = r.GetBit(key, 1026)
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("want 0, got %d", val)
	}
}

func TestRedis_HMGet(t *testing.T) {
	key := []byte("u:12345")
	pool, err := group.Index(0)
	if err != nil {
		t.Fatal(err.Error())
	}

	r := pool.Get()
	defer pool.Put(r)

	ok, err := r.HMSet(key, map[string]interface{}{
		"id":       12345,
		"nickname": "苍穹",
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ok {
		t.Fatal("want true, got false")
	}

	values, err := r.HMGet(key, []string{"nickname", "id"})
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(values) != 2 {
		t.Fatalf("want 2, got %d", len(values))
	}

	if values[0] != "苍穹" {
		t.Fatalf(`want 苍穹, got %s`, values[0])
	}

	if values[1] != "12345" {
		t.Fatalf(`want 12345, got %s`, values[1])
	}

	mValues, err := r.HMGetMap(key, []string{"nickname", "id"})
	if err != nil {
		t.Fatal(err.Error())
	}
	if mValues["nickname"] != "苍穹" {
		t.Fatalf(`want 苍穹, got %s`, mValues["nickname"])
	}

	if mValues["id"] != "12345" {
		t.Fatalf(`want 12345, got %s`, mValues["id"])
	}
}
