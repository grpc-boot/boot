package etcd

import (
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
)

func BenchmarkGet(b *testing.B) {
	cli, err := NewClient(&clientv3.Config{
		Endpoints:         []string{"10.16.49.131:2379"},
		AutoSyncInterval:  0,
		DialTimeout:       time.Second * 3,
		DialKeepAliveTime: 60,
		Username:          "",
		Password:          "",
	}, []string{"config"}, clientv3.WithPrefix())

	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	b.ResetTimer()

	var exists bool

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, exists = cli.Get("config/mini/db")
			if !exists {
				b.Fatal("want true, got false")
			}
		}
	})
}
