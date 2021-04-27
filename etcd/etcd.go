package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/v3"
)

type Service interface {
	//注册服务
	Register(serviceTarget string, value string)
	//获取所有服务列表
	Get(serviceTarget string) (list map[string]interface{}, exists bool)
	//随机取一个服务
	RandOne(key string) (value interface{}, exists bool)
	//遍历服务，handler返回true时停止遍历
	Range(key string, handler func(index string, val interface{}) (handled bool))
	Close() (err error)
}

func NewService(conf *clientv3.Config, prefix string, opts ...clientv3.OpOption) (s Service, err error) {
	var serv *service
	serv, err = newService(conf)
	if err != nil {
		return
	}
	serv.prefix = prefix

	var resp *clientv3.GetResponse

	resp, err = serv.client.Get(context.Background(), serv.prefix, clientv3.WithPrefix())
	if err != nil {
		return
	}

	serv.mutex.Lock()
	defer serv.mutex.Unlock()

	for _, ev := range resp.Kvs {
		serviceTarget, index := serv.key2Target(string(ev.Key))
		if serviceTarget == "" || index == "" {
			continue
		}

		if _, exists := serv.service[serviceTarget]; !exists {
			serv.service[serviceTarget] = make(map[string]interface{})
		}
		serv.service[serviceTarget][index] = ev.Value
	}

	serv.watch(opts...)

	return serv, err
}

type service struct {
	Service

	mutex   sync.RWMutex
	prefix  string
	service map[string]map[string]interface{}
	client  *clientv3.Client
}

func newService(conf *clientv3.Config) (c *service, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*conf)

	if err != nil {
		return nil, err
	}

	c = &service{
		client:  conn,
		service: make(map[string]map[string]interface{}),
	}
	return
}

func (s *service) key2Target(key string) (serviceTarget, index string) {
	if (len(s.prefix) + 4) > len(key) {
		return
	}

	if strings.Index(key, s.prefix) != 0 {
		return
	}

	serviceSuffix := key[len(s.prefix)+1:]
	keyEnd := strings.Index(serviceSuffix, "/")
	if keyEnd < 1 || keyEnd == len(serviceSuffix) {
		return
	}

	serviceTarget = serviceSuffix[:keyEnd]
	index = serviceSuffix[keyEnd+1:]
	return
}

func (s *service) watch(opts ...clientv3.OpOption) {
	go func() {
		watchanel := s.client.Watch(context.Background(), s.prefix, opts...)
		for watchResponse := range watchanel {
			for _, ev := range watchResponse.Events {
				switch ev.Type {
				case mvccpb.PUT:
					s.addService(string(ev.Kv.Key), ev.Kv.Value)
				case mvccpb.DELETE:
					s.delService(string(ev.Kv.Key))
				}
			}
		}
	}()
}

func (s *service) addService(key string, value interface{}) {
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exists := s.service[serviceTarget]; !exists {
		s.service[serviceTarget] = make(map[string]interface{})
	}
	s.service[serviceTarget][index] = value
}

func (s *service) delService(key string) {
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.service[serviceTarget]; !exists {
		return
	}

	delete(s.service[serviceTarget], index)
}

func (s *service) Register(serviceTarget string, value string) {
	serviceTarget = fmt.Sprintf(s.prefix+"/%s/", serviceTarget)
	var (
		kv                          = clientv3.NewKV(s.client)
		lease                       = clientv3.NewLease(s.client)
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

func (s *service) Get(key string) (list map[string]interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var items map[string]interface{}
	items, exists = s.service[key]
	if exists {
		//拷贝，否则有并发问题
		list = make(map[string]interface{}, len(items))
		for index, value := range list {
			list[index] = value
		}
	}
	return
}

func (s *service) RandOne(key string) (value interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var items map[string]interface{}
	items, exists = s.service[key]
	if exists {
		for _, val := range items {
			return val, exists
		}
	}
	return
}

func (s *service) Range(key string, handler func(index string, val interface{}) (handled bool)) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	items, exists := s.service[key]
	if exists {
		for index, val := range items {
			if handler(index, val) {
				return
			}
		}
	}
}

func (s *service) Close() (err error) {
	return s.client.Close()
}

type Client interface {
	Watch(key string, opts ...clientv3.OpOption)
	Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error)
	Get(key string) (value interface{}, exists bool)
	GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error)
	Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error)
	Connection() (client *clientv3.Client)
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

	cache  sync.Map
	client *clientv3.Client
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

func (c *client) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Put(ctx, key, value, opts...)
}

func (c *client) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Delete(ctx, key, opts...)
}

func (c *client) Connection() (client *clientv3.Client) {
	return c.client
}

func (c *client) Close() (err error) {
	return c.client.Close()
}
