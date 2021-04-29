package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/grpc-boot/boot/grace"
	"github.com/grpc-boot/boot/rocket-mq"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

var (
	c *rocket_mq.PushConsumer
)

func retry(msg *primitive.MessageExt) (consumer.ConsumeResult, error) {
	if msg.ReconsumeTimes > 10 {
		log.Printf("msg:%s retry:%d\n", string(msg.Body), msg.ReconsumeTimes)
	}
	if rand.Intn(100) > 50 {
		return consumer.ConsumeSuccess, nil
	}

	return consumer.ConsumeRetryLater, nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	c, _ = rocket_mq.NewPushConsumer(
		consumer.WithGroupName("boot"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.16.49.131:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeMessageBatchMaxSize(1),
	)

	err := c.Subscribe("boot", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		log.Printf("subscribe error:%s\n", err.Error())
	}
	// Note: start after subscribe
	err = c.Start()
	if err != nil {
		log.Fatalf("start error:%s\n", err.Error())
	}

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		err = c.Shutdown()
		if err != nil {
			log.Fatalf("shundown Consumer error: %s", err.Error())
		}
		return
	})
	hold.Start()
}
