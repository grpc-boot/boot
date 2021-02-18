package kafka

import (
	"fmt"
	"github.com/grpc-boot/boot"
)

var (
	conf     *Config
	producer *Producer
	consumer *Consumer
	err      error

	topics = []string{"dds"}
)

type Config struct {
	ProducerOption Option `yaml:"producer" json:"producer"`
	ConsumerOption Option `yaml:"consumer" json:"consumer"`
}

func init() {
	boot.Yaml("./app.yml", &conf)
	producer, err = NewProducer(&conf.ProducerOption)
	if err != nil {
		panic(err.Error())
	}

	consumer, err = NewConsumer(&conf.ProducerOption)
	if err != nil {
		panic(err.Error())
	}
}

func ExampleConsumer_RunConsume() {
	if err := consumer.Subscribe(topics, nil); err != nil {
		panic(err.Error())
	}

	defer func() {
		consumer.StopConsume()
		consumer.StopConsume()
	}()

	consumer.RunConsume(true, func(topic string, msg []byte, err error) {
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Printf("topic:%s, msg: %s\n", topic, string(msg))
	})
}
