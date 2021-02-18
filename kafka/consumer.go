package kafka

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	librdkafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Option struct {
	Properties librdkafka.ConfigMap `yaml:"properties" json:"properties"`
}

type Consumer struct {
	consumer *librdkafka.Consumer
	run      uint32
	wg       *sync.WaitGroup
}

func NewConsumer(option *Option) (consumer *Consumer, err error) {
	var cons *librdkafka.Consumer
	cons, err = librdkafka.NewConsumer(&option.Properties)
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer: cons}, nil
}

func (c *Consumer) RunConsume(lockOsThread bool, handler func(topic string, msg []byte, err error)) {
	go func() {
		if lockOsThread {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
		}

		var (
			topic string
			msg   []byte
			err   error
		)

		defer func() {
			if err := recover(); err != nil {
				log.Println("error:", err)
			}
		}()

		atomic.StoreUint32(&c.run, 1)
		c.wg.Add(1)

		for {
			topic, msg, err = c.ReadBytes(-1)
			handler(topic, msg, err)

			if atomic.LoadUint32(&c.run) == 0 {
				break
			}
		}
		c.wg.Done()
	}()
}

func (c *Consumer) StopConsume() {
	atomic.StoreUint32(&c.run, 0)
	c.wg.Wait()
}

func (c *Consumer) Subscribe(topics []string, cb librdkafka.RebalanceCb) (err error) {
	return c.consumer.SubscribeTopics(topics, cb)
}

func (c *Consumer) Unsubscribe() (err error) {
	return c.consumer.Unsubscribe()
}

func (c *Consumer) Read(timeout time.Duration) (msg *librdkafka.Message, err error) {
	return c.consumer.ReadMessage(timeout)
}

func (c *Consumer) ReadString(timeout time.Duration) (topic, msg string, err error) {
	var message *librdkafka.Message
	message, err = c.consumer.ReadMessage(timeout)
	if err != nil {
		return "", "", err
	}
	return *message.TopicPartition.Topic, string(message.Value), nil
}

func (c *Consumer) ReadBytes(timeout time.Duration) (topic string, msg []byte, err error) {
	var message *librdkafka.Message
	message, err = c.consumer.ReadMessage(timeout)
	if err != nil {
		return "", nil, err
	}
	return *message.TopicPartition.Topic, message.Value, nil
}

func (c *Consumer) Assign(partitions ...librdkafka.TopicPartition) (err error) {
	return c.consumer.Assign(partitions)
}

func (c *Consumer) UnAssign() (err error) {
	return c.consumer.Unassign()
}

func (c *Consumer) Close() (err error) {
	return c.consumer.Close()
}
