package boot

import (
	"encoding/json"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

func Yaml(filePath string, out interface{}) (err error) {
	conf, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(conf, out)
}

func Json(filePath string, out interface{}) (err error) {
	conf, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	return json.Unmarshal(conf, out)
}
