package kafka

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	librdkafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	msgPool = sync.Pool{
		New: func() interface{} {
			return &librdkafka.Message{
				TopicPartition: librdkafka.TopicPartition{},
				Value:          make([]byte, 0, 32),
			}
		},
	}

	msgGet = func() *librdkafka.Message {
		return msgPool.Get().(*librdkafka.Message)
	}

	msgPut = func(msg *librdkafka.Message) {
		msg.Value = msg.Value[:0]
		msg.Headers = nil
		msg.Key = nil
		msg.TopicPartition = librdkafka.TopicPartition{}
		msg.Opaque = nil
		msg.Timestamp = time.Time{}
		msg.TimestampType = 0
		msgPool.Put(msg)
	}

	BuildMsg = func(topic *string, msg []byte) (message *librdkafka.Message) {
		message = msgGet()
		message.TopicPartition.Topic = topic
		message.Value = msg
		message.Timestamp = time.Now()
		message.TimestampType = librdkafka.TimestampCreateTime
		return
	}
)

type Producer struct {
	successCount uint64
	buffer       chan *librdkafka.Message
	producer     *librdkafka.Producer
}

func NewProducer(option *Option) (producer *Producer, err error) {
	var prod *librdkafka.Producer
	prod, err = librdkafka.NewProducer(&option.Properties)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: prod,
		buffer:   prod.ProduceChannel(),
	}, nil
}

func (p *Producer) ProduceByBuffer(msg *librdkafka.Message) {
	p.buffer <- msg
}

func (p *Producer) Produce(msg *librdkafka.Message, deliveryChan chan librdkafka.Event) (err error) {
	return p.producer.Produce(msg, deliveryChan)
}

func (p *Producer) ProduceString(topic *string, msg string, deliveryChan chan librdkafka.Event) (err error) {
	return p.Produce(BuildMsg(topic, []byte(msg)), deliveryChan)
}

func (p *Producer) ProduceBytes(topic *string, msg []byte, deliveryChan chan librdkafka.Event) (err error) {
	return p.Produce(BuildMsg(topic, msg), deliveryChan)
}

func (p *Producer) Flush(timeoutMs int) {
	p.producer.Flush(timeoutMs)
}

func (p *Producer) Purge(flags int) (err error) {
	return p.producer.Purge(flags)
}

func (p *Producer) Begin() (err error) {
	return p.producer.BeginTransaction()
}

func (p *Producer) Rollback(ctx context.Context) (err error) {
	return p.producer.AbortTransaction(ctx)
}

func (p *Producer) Commit(ctx context.Context) (err error) {
	return p.producer.CommitTransaction(ctx)
}

func (p *Producer) InitTrans(ctx context.Context) (err error) {
	return p.producer.InitTransactions(ctx)
}

func (p *Producer) ErrorHandler(handler func(msg *librdkafka.Message, err error)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("error:", err)
			}
		}()

		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *librdkafka.Message:
				if ev.TopicPartition.Error != nil {
					handler(ev, ev.TopicPartition.Error)
				} else {
					atomic.AddUint64(&p.successCount, 1)
					msgPut(ev)
				}
			}
		}
	}()
}

func (p *Producer) Close() {
	p.producer.Close()
}