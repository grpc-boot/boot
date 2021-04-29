package main

import (
	"context"
	"log"

	"github.com/grpc-boot/boot/grace"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("boot"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.16.49.131:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
	)

	delayLevel := 1
	err := c.Subscribe("boot", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		//log.Printf("subscribe callback len: %d \n", len(msgs))

		concurrentCtx, _ := primitive.GetConcurrentlyCtx(ctx)
		concurrentCtx.DelayLevelWhenNextConsume = delayLevel // only run when return consumer.ConsumeRetryLater

		for _, msg := range msgs {
			if msg.ReconsumeTimes > 105 {
				log.Printf("msg ReconsumeTimes > 5. msg: %v", msg)
				return consumer.ConsumeSuccess, nil
			} else {
				//log.Printf("subscribe callback: %v \n", msg)
			}
		}
		return consumer.ConsumeRetryLater, nil
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
