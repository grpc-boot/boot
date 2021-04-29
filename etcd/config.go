package etcd

import (
	"context"
	"time"

	"github.com/grpc-boot/boot/container"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

type Client interface {
	Watch(key string, opts ...clientv3.OpOption)
	Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error)
	Get(key string) (value interface{}, exists bool)
	GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error)
	Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error)
	Connection() (client *clientv3.Client)
	Close() (err error)
}

func NewClient(conf *clientv3.Config, prefixes []string, keyItems map[string]Deserialize, opts ...clientv3.OpOption) (c Client, err error) {
	var cli *client
	cli, err = newClient(conf, keyItems)
	if err != nil {
		return
	}

	var resp *clientv3.GetResponse
	for _, prefix := range prefixes {
		resp, err = cli.connection.Get(context.Background(), prefix, clientv3.WithPrefix())
		if err != nil {
			return
		}

		for _, ev := range resp.Kvs {
			key := string(ev.Key)
			if value, er := deserialize(&cli.deserializers, key, ev.Value); er == nil {
				cli.cache.Set(key, value)
			}
		}

		cli.Watch(prefix, opts...)
	}
	return cli, err
}

type client struct {
	Client

	cache         *container.Map
	connection    *clientv3.Client
	deserializers map[string]Deserialize
}

func newClient(conf *clientv3.Config, deserializers map[string]Deserialize) (c *client, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*conf)

	if err != nil {
		return nil, err
	}

	c = &client{
		connection:    conn,
		cache:         container.NewMap(),
		deserializers: deserializers,
	}
	return
}

func (c *client) Watch(key string, opts ...clientv3.OpOption) {
	go func() {
		watchanel := c.connection.Watch(context.Background(), key, opts...)
		for watchResponse := range watchanel {
			for _, ev := range watchResponse.Events {
				switch ev.Type {
				case mvccpb.PUT:
					k := string(ev.Kv.Key)
					if value, err := deserialize(&c.deserializers, k, ev.Kv.Value); err == nil {
						c.cache.Set(k, value)
					}
				case mvccpb.DELETE:
					c.cache.Delete(ev.Kv)
				}
			}
		}
	}()
}

func (c *client) Get(key string) (value interface{}, exists bool) {
	return c.cache.Get(key)
}

func (c *client) GetGetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var resp *clientv3.GetResponse
	resp, err = c.connection.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *client) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.connection.Put(ctx, key, value, opts...)
}

func (c *client) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.connection.Delete(ctx, key, opts...)
}

func (c *client) Connection() (client *clientv3.Client) {
	return c.connection
}

func (c *client) Close() (err error) {
	return c.connection.Close()
}
