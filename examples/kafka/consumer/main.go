package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/grpc-boot/boot/grace"
	"github.com/grpc-boot/boot/kafka"
)

var (
	conf     *Config
	consumer *kafka.Consumer
	confJson = `{
			"topics":["browser_test_topic"], 
			"consumer":{
				"properties":{
					"bootstrap.servers":"127.0.0.1:39092", 
					"group.id":"myGroup", 
					"auto.offset.reset":"earliest"
				}
			}
	}`
)

type Config struct {
	Topics         []string     `yaml:"topics" json:"topics"`
	ConsumerOption kafka.Option `yaml:"consumer" json:"consumer"`
}

func init() {
	err := json.Unmarshal([]byte(confJson), &conf)
	if err != nil {
		panic(err.Error())
	}

	consumer, err = kafka.NewConsumer(&conf.ConsumerOption)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	if err := consumer.Subscribe(conf.Topics, nil); err != nil {
		panic(err.Error())
	}

	consumer.RunConsume(true, time.Second, func(topic string, msg []byte, err error) {
		if err != nil {
			if err.Error() != kafka.ErrLocalTimeout.Error() {
				log.Println(err.Error())
			}
			return
		}

		log.Printf("topic:%s, msg: %s\n", topic, string(msg))
	})

	hold := grace.NewHold(func(ctx context.Context) (err error) {
		consumer.StopConsume()
		return consumer.Close()
	})
	hold.Start()
}
