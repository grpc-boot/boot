package boot

import (
	"encoding/json"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

func Yaml(filePath string, out interface{}) {
	conf, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err.Error())
	}

	err = yaml.Unmarshal(conf, out)
	if err != nil {
		panic(err.Error())
	}
}

func Json(filePath string, out interface{}) {
	conf, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err.Error())
	}

	err = json.Unmarshal(conf, out)
	if err != nil {
		panic(err.Error())
	}
}
