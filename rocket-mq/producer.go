package rocket_mq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type Producer struct {
	connection rocketmq.Producer
}

func NewProducer(opts ...producer.Option) (producer *Producer, err error) {
	var conn rocketmq.Producer
	conn, err = rocketmq.NewProducer(opts...)
	if err != nil {
		return
	}

	producer = &Producer{
		connection: conn,
	}
	return
}

func (p *Producer) Start() (err error) {
	return p.connection.Start()
}

func (p *Producer) Send(ctx context.Context, mq ...*primitive.Message) (result *primitive.SendResult, err error) {
	return p.connection.SendSync(ctx, mq...)
}

func (p *Producer) SendOneWay(ctx context.Context, mq ...*primitive.Message) (err error) {
	return p.connection.SendOneWay(ctx, mq...)
}

func (p *Producer) SendAsync(ctx context.Context, mq []*primitive.Message, callback func(ctx context.Context, result *primitive.SendResult, err error)) (err error) {
	return p.connection.SendAsync(ctx, callback, mq...)
}

func (p *Producer) Shutdown() (err error) {
	return p.Close()
}

func (p *Producer) Close() (err error) {
	return p.connection.Shutdown()
}
