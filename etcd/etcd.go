package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3"
)

var ServiceKeyPrefix = "/boot/services/%s/"

type Client interface {
	Watch(key string, opts ...clientv3.OpOption)
	Register(serviceTarget string, value string)
	GetService(serviceTarget string) (list []string, exists bool)
	Get(key string) (value interface{}, exists bool)
	GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error)
	Close() (err error)
}

func NewClient(conf *clientv3.Config, prefixes []string, opts ...clientv3.OpOption) (c Client, err error) {
	var cli *client
	cli, err = newClient(conf)
	if err != nil {
		return
	}

	var resp *clientv3.GetResponse
	for _, prefix := range prefixes {
		resp, err = cli.client.Get(context.Background(), prefix, clientv3.WithPrefix())
		if err != nil {
			return
		}

		for _, ev := range resp.Kvs {
			cli.cache.Store(string(ev.Key), ev.Value)
		}

		cli.Watch(prefix, opts...)
	}
	return cli, err
}

type client struct {
	Client

	cache   sync.Map
	service sync.Map
	client  *clientv3.Client
}

func newClient(conf *clientv3.Config) (c *client, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*conf)

	if err != nil {
		return nil, err
	}

	c = &client{
		client: conn,
	}
	return
}

func (c *client) Watch(key string, opts ...clientv3.OpOption) {
	go func() {
		watchanel := c.client.Watch(context.Background(), key, opts...)
		for watchResponse := range watchanel {
			for _, ev := range watchResponse.Events {
				switch ev.Type {
				case mvccpb.PUT:
					c.cache.Store(string(ev.Kv.Key), ev.Kv.Value)
				case mvccpb.DELETE:
					c.cache.Delete(string(ev.Kv.Key))
				}
			}
		}
	}()
}

func (c *client) Register(serviceTarget string, value string) {
	serviceTarget = fmt.Sprintf(ServiceKeyPrefix, serviceTarget)
	var (
		kv                          = clientv3.NewKV(c.client)
		lease                       = clientv3.NewLease(c.client)
		curLeaseId clientv3.LeaseID = 0
		leaseResp  *clientv3.LeaseGrantResponse
		err        error
	)

	go func() {
		tick := time.NewTicker(time.Second)
		for range tick.C {
			if curLeaseId == 0 {
				leaseResp, err = lease.Grant(context.TODO(), 10)
				if err != nil {
					continue
				}

				key := serviceTarget + fmt.Sprintf("%d", leaseResp.ID)
				if _, err = kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID)); err != nil {
					continue
				}
				curLeaseId = leaseResp.ID
			} else {
				if _, err = lease.KeepAliveOnce(context.TODO(), curLeaseId); err == rpctypes.ErrLeaseNotFound {
					curLeaseId = 0
					continue
				}
			}
		}
	}()
}

func (c *client) Get(key string) (value interface{}, exists bool) {
	return c.cache.Load(key)
}

func (c *client) GetGetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var resp *clientv3.GetResponse
	resp, err = c.client.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *client) Close() (err error) {
	return c.client.Close()
}
