package etcd

import (
	"github.com/json-iterator/go"
	"gopkg.in/yaml.v2"
)

var (
	DefaultJsonMapDeserialize Deserialize = func(data []byte) (value interface{}, err error) {
		var val map[string]interface{}
		err = jsoniter.Unmarshal(data, &val)
		return val, err
	}

	DefaultYamlMapDeserialize Deserialize = func(data []byte) (value interface{}, err error) {
		var val map[string]interface{}
		err = yaml.Unmarshal(data, &val)
		return val, err
	}

	DefaultStringDeserialize Deserialize = func(data []byte) (value interface{}, err error) {
		return string(data), nil
	}
)

type Deserialize func(data []byte) (value interface{}, err error)

func deserialize(deserializers *map[string]Deserialize, key string, val []byte) (value interface{}, err error) {
	if deserializers == nil {
		return val, err
	}

	if deserializer, exists := (*deserializers)[key]; exists {
		value, err = deserializer(val)
		return value, err
	}
	return val, err
}
