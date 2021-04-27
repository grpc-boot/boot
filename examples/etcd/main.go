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
	s          etcd.Service
	c          etcd.Client
	configData = map[string]interface{}{
		"config/cloud":   `{"ver":"3.4.5", "updated_at":%d}`,
		"config/mini/db": `{"ver":"3.4.5", "updated_at":%d}`,
	}
)

func logService() {
	tick := time.NewTicker(time.Second)
	for range tick.C {
		s.Range("account", func(index string, val interface{}) (handled bool) {
			log.Printf("index:%s, value:%s\n", index, val)
			return
		})
	}
}

func register() {
	var err error
	s, err = etcd.NewService(conf, "boot/service", clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	s.Register("account", `{"host":"127.0.0.1", "port":4567}`)
}

func client() {
	var err error
	c, err = etcd.NewClient(conf, []string{"config"}, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		tick := time.NewTicker(time.Second)
		for range tick.C {
			for key, value := range configData {
				c.Put(key, fmt.Sprintf(value.(string), time.Now().Unix()), time.Second)
			}
		}
	}()
}

func logConf() {
	tick := time.NewTicker(time.Second)
	for range tick.C {
		for key, _ := range configData {
			val, _ := c.Get(key)
			log.Printf("key:%s, value:%s\n", key, val)
		}
	}
}

func main() {
	//register()
	//go logService()

	client()
	go logConf()

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		return
	})
	hold.Start()
}
