package main

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"log"
	"strconv"
	"time"

	"github.com/grpc-boot/boot/grace"
	"github.com/grpc-boot/boot/rocket-mq"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

var (
	p *rocket_mq.Producer
)

func tickProduce(topic string) {
	tick := time.NewTicker(time.Second)
	var i int
	for range tick.C {
		i++
		msg := &primitive.Message{
			Topic: topic,
			Body:  []byte("Hello RocketMQ Go Client! " + strconv.Itoa(i)),
		}
		res, err := p.Send(context.Background(), msg)

		if err != nil {
			log.Printf("send message error: %s\n", err)
		} else {
			log.Printf("send message success: result=%s\n", res.String())
		}
	}
}

func benchProduce(topic string) {
	for {
		body, _ := jsoniter.Marshal(fmt.Sprintf(`{"id": 1, "created_at":%d}`, time.Now().UnixNano()))
		msg := &primitive.Message{
			Topic: topic,
			Body:  body,
		}

		_, err := p.Send(context.Background(), msg)
		if err != nil {
			log.Printf("send message error: %s\n", err)
		}
	}
}

func benchAsyncProduce(topic string) {
	for {
		body, _ := jsoniter.Marshal(fmt.Sprintf(`{"id": 1, "created_at":%d}`, time.Now().UnixNano()))
		msg := &primitive.Message{
			Topic: topic,
			Body:  body,
		}

		msgList := []*primitive.Message{msg}

		err := p.SendAsync(context.Background(), msgList, func(ctx context.Context, result *primitive.SendResult, err error) {
			if err != nil {
				log.Printf("async produce failed msgId:%s\n", result.MsgID)
			}
		})

		if err != nil {
			log.Printf("send message error: %s\n", err)
		}
	}
}

func main() {
	p, _ = rocket_mq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"10.16.49.131:9876"})),
		producer.WithGroupName("boot"),
		producer.WithRetry(2),
	)

	err := p.Start()
	if err != nil {
		log.Fatalf("start producer error: %s", err.Error())
	}

	go benchAsyncProduce("boot")
	go benchAsyncProduce("boot")

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		err = p.Shutdown()
		if err != nil {
			log.Printf("shutdown producer error: %s", err.Error())
		}
		return
	})
	hold.Start()
}
