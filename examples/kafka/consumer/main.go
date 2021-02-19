package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/grpc-boot/boot"
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
				fmt.Println(err.Error())
			}
			return
		}

		fmt.Printf("topic:%s, msg: %s\n", topic, string(msg))
	})

	boot.Wait(time.Second*5, func(ctx context.Context) {
		consumer.StopConsume()
		_ = consumer.Close()
	})
}
