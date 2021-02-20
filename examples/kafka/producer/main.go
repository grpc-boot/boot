package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/grpc-boot/boot/grace"
	"github.com/grpc-boot/boot/kafka"

	librdkafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	conf     *Config
	producer *kafka.Producer
	msgBytes = []byte(`{"userId": 1234, "userName":"nickName"}`)
	confJson = `{
			"topics":["browser_test_topic"], 
			"producer":{
				"properties":{
					"bootstrap.servers":"127.0.0.1:39092",
					"go.produce.channel.size": 40960,
					"queue.buffering.max.messages": 409600,
					"queue.buffering.max.ms": 5000,
					"message.timeout.ms": 2147483647
				}
			}
	}`
)

type Config struct {
	Topics         []string     `yaml:"topics" json:"topics"`
	ProducerOption kafka.Option `yaml:"producer" json:"producer"`
}

func init() {
	err := json.Unmarshal([]byte(confJson), &conf)
	if err != nil {
		panic(err.Error())
	}

	producer, err = kafka.NewProducer(&conf.ProducerOption)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	start := time.Now()

	dEventChan := make(chan librdkafka.Event, 1024)

	producer.ErrorHandler(dEventChan, func(msg *librdkafka.Message, err error) {
		switch err {
		case kafka.ErrQueueFull:
			//
		case kafka.ErrLocalTimeout:
			//重试
			_ = producer.Produce(msg, dEventChan)
		default:
			log.Println(err.Error())
		}
	})

	go func() {
		var index uint64 = 0
		for {
			err := producer.Produce(kafka.BuildMsg(&conf.Topics[0], msgBytes), dEventChan)
			if err == nil {
				index++
				continue
			}

			switch err.Error() {
			case kafka.ErrProducerHasClosed.Error():
				log.Println("closed:", index)
				return
			case kafka.ErrQueueFull.Error():
				log.Println("full:", index)
				return
			case kafka.ErrLocalTimeout.Error():
				continue
			default:
				log.Println(err.Error())
			}
		}
	}()

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		producer.CloseIn()
		producer.Close(15 * 1000)
		close(dEventChan)

		log.Println("cost ", time.Now().Sub(start), " produce success:", producer.SuccessCount())
		return
	})
	hold.Start()
}
