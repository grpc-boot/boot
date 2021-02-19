package kafka

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/grpc-boot/boot/atomic"

	librdkafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Option struct {
	Properties map[string]interface{} `yaml:"properties" json:"properties"`
}

func convertProperties2ConfigMap(properties map[string]interface{}) *librdkafka.ConfigMap {
	configMap := &librdkafka.ConfigMap{}
	for property, value := range properties {
		//json解析的时候，数字默认解析为float64
		if val, ok := value.(float64); ok {
			_ = configMap.SetKey(property, int(val))
			continue
		}

		_ = configMap.SetKey(property, value)
	}
	return configMap
}

type Consumer struct {
	consumer *librdkafka.Consumer
	run      atomic.Acquire
	wg       sync.WaitGroup
}

func NewConsumer(option *Option) (consumer *Consumer, err error) {
	var cons *librdkafka.Consumer
	cons, err = librdkafka.NewConsumer(convertProperties2ConfigMap(option.Properties))
	if err != nil {
		return nil, err
	}

	return &Consumer{consumer: cons}, nil
}

func (c *Consumer) RunConsume(lockOsThread bool, timeout time.Duration, handler func(topic string, msg []byte, err error)) {
	if !c.run.Acquire() {
		return
	}
	c.wg.Add(1)

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

		for {
			topic, msg, err = c.ReadBytes(timeout)
			handler(topic, msg, err)

			if c.run.IsRelease() {
				break
			}
		}
		c.wg.Done()
	}()
}

func (c *Consumer) StopConsume() {
	c.run.Release()
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
