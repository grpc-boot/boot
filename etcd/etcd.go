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
	Register(serviceTarget string, value string)
	Get(serviceTarget string) (value interface{}, exists bool)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

	if _, exists := s.service[serviceTarget]; !exists {
		s.service[serviceTarget] = make(map[string]interface{})
	}
	s.service[serviceTarget][index] = value
}

func (s *service) delService(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

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

func (s *service) Get(key string) (value interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.Unlock()
	value, exists = s.service[key]
	return
}

func (s *service) Close() (err error) {
	return s.client.Close()
}

type Client interface {
	Watch(key string, opts ...clientv3.OpOption)
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

func (c *client) Close() (err error) {
	return c.client.Close()
}
