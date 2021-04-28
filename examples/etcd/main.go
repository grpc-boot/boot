package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/grpc-boot/boot/etcd"
	"github.com/grpc-boot/boot/grace"

	"go.etcd.io/etcd/client/v3"
)

var (
	conf = &clientv3.Config{
		Endpoints:         []string{"10.16.49.131:2379"},
		AutoSyncInterval:  0,
		DialTimeout:       time.Second * 3,
		DialKeepAliveTime: 10,
		Username:          "",
		Password:          "",
	}

	clientDeSerializer = map[string]etcd.Deserialize{
		"config/cloud":     etcd.DefaultJsonMapDeserialize,
		"config/mini/db":   etcd.DefaultJsonMapDeserialize,
		"config/reco/list": etcd.DefaultJsonMapDeserialize,
	}

	serviceDeSerializer = map[string]etcd.Deserialize{
		"account": etcd.DefaultJsonMapDeserialize,
	}

	s, _ = etcd.NewService(conf, "boot/service", serviceDeSerializer, clientv3.WithPrefix())
	c, _ = etcd.NewClient(conf, []string{"config"}, clientDeSerializer, clientv3.WithPrefix())

	configData = map[string]interface{}{
		"config/cloud":   `{"ver":"3.4.5", "updated_at":%d}`,
		"config/mini/db": `{"ver":"3.4.5", "updated_at":%d}`,
	}
)

func logService() {
	tick := time.NewTicker(time.Second)
	for range tick.C {
		s.Range("account", func(index string, val interface{}) (handled bool) {
			log.Printf("index:%s, value:%v\n", index, val)
			return
		})
	}
}

func logConf() {
	tick := time.NewTicker(time.Second)
	for range tick.C {
		for key, _ := range configData {
			val, _ := c.Get(key)
			log.Printf("key:%s, value:%v\n", key, val)
		}

		val, _ := c.Get("config/reco/lis")
		log.Printf("key:%s, value:%v\n", "config/reco/lis", val)
	}
}

func main() {
	defer func() {
		_ = c.Close()
		_ = s.Close()
	}()

	go logService()
	go logConf()

	s.Register("account", `{"host":"10.16.49.131", "port":8090, "protocal":"grpc"}`)

	go func() {
		tick := time.NewTicker(time.Second)
		for range tick.C {
			for key, value := range configData {
				c.Put(key, fmt.Sprintf(value.(string), time.Now().Unix()), time.Second)
			}
		}
	}()

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		return
	})
	hold.Start()
}
