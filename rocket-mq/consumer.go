package rocket_mq

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type PushConsumer struct {
	connection rocketmq.PushConsumer
}

func NewPushConsumer(opts ...consumer.Option) (pushConsumer *PushConsumer, err error) {
	var connection rocketmq.PushConsumer
	connection, err = rocketmq.NewPushConsumer(opts...)
	if err != nil {
		return
	}
	pushConsumer = &PushConsumer{connection: connection}
	return
}

func (pc *PushConsumer) Shutdown() (err error) {
	return pc.Close()
}

func (pc *PushConsumer) Close() (err error) {
	return pc.connection.Shutdown()
}

func (pc *PushConsumer) Start() (err error) {
	return pc.connection.Start()
}

func (pc *PushConsumer) Unsubscribe(topic string) (err error) {
	return pc.connection.Unsubscribe(topic)
}

func (pc *PushConsumer) Subscribe(topic string, selector consumer.MessageSelector, handler func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error)) (err error) {
	return pc.connection.Subscribe(topic, selector, handler)
}
