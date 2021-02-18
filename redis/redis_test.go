package redis

import (
	"fmt"
	"testing"

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
	pool, err := group.Get([]byte("user:1234"))
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(string(pool.id))
}
