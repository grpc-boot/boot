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
