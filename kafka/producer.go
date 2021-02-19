package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/grpc-boot/boot/atomic"

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
		message.TopicPartition.Partition = librdkafka.PartitionAny
		message.Value = msg
		message.Timestamp = time.Now()
		message.TimestampType = librdkafka.TimestampCreateTime
		return
	}
)

var (
	ErrLocalTimeout      = errors.New("Local: Timed out")
	ErrProducerHasClosed = errors.New("Local: Producer Has Closed")
	ErrQueueFull         = errors.New("Local: Queue full")
)

type Producer struct {
	successCount atomic.Uint64
	producer     *librdkafka.Producer
	isClose      atomic.Bool
}

func NewProducer(option *Option) (producer *Producer, err error) {
	var prod *librdkafka.Producer
	prod, err = librdkafka.NewProducer(convertProperties2ConfigMap(option.Properties))
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: prod,
	}, nil
}

func (p *Producer) Produce(msg *librdkafka.Message, deliveryChan chan librdkafka.Event) (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}

	return p.producer.Produce(msg, deliveryChan)
}

func (p *Producer) ProduceString(topic *string, msg string, deliveryChan chan librdkafka.Event) (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}
	return p.Produce(BuildMsg(topic, []byte(msg)), deliveryChan)
}

func (p *Producer) ProduceBytes(topic *string, msg []byte, deliveryChan chan librdkafka.Event) (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}
	return p.Produce(BuildMsg(topic, msg), deliveryChan)
}

func (p *Producer) Flush(timeoutMs int) {
	p.producer.Flush(timeoutMs)
}

func (p *Producer) Purge(flags int) (err error) {
	return p.producer.Purge(flags)
}

func (p *Producer) Begin() (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}

	return p.producer.BeginTransaction()
}

func (p *Producer) Rollback(ctx context.Context) (err error) {
	return p.producer.AbortTransaction(ctx)
}

func (p *Producer) Commit(ctx context.Context) (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}
	return p.producer.CommitTransaction(ctx)
}

func (p *Producer) InitTrans(ctx context.Context) (err error) {
	if p.isClose.Get() {
		return ErrProducerHasClosed
	}
	return p.producer.InitTransactions(ctx)
}

func (p *Producer) ErrorHandler(deliveryChan chan librdkafka.Event, handler func(msg *librdkafka.Message, err error)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("error:", err)
			}
		}()

		if deliveryChan == nil {
			deliveryChan = p.producer.Events()
		}

		for {
			e, hasOpen := <-deliveryChan
			if !hasOpen {
				break
			}

			switch ev := e.(type) {
			case *librdkafka.Message:
				if ev.TopicPartition.Error == nil {
					p.successCount.Incr(1)
					msgPut(ev)
					continue
				}

				switch ev.TopicPartition.Error.Error() {
				case ErrQueueFull.Error():
					handler(ev, ErrQueueFull)
				case ErrLocalTimeout.Error():
					handler(ev, ErrLocalTimeout)
				case ErrQueueFull.Error():
					handler(ev, ErrQueueFull)
				default:
					handler(ev, ev.TopicPartition.Error)
				}
			}
		}
	}()
}

func (p *Producer) Close(timeoutMs int) {
	p.isClose.Set(true)
	p.producer.Flush(timeoutMs)
	p.producer.Close()
}

func (p *Producer) SuccessCount() uint64 {
	return p.successCount.Get()
}

func (p *Producer) CloseIn() {
	p.isClose.Set(true)
}
