package personas

import (
	"strconv"
	"testing"
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/redis"
)

var (
	conf       *Conf
	redisGroup *redis.Group
	personas   *Personas
)

type Conf struct {
	Option  Option         `yaml:"option" json:"option"`
	Storage []redis.Option `yaml:"storage" json:"storage"`
}

func init() {
	conf = new(Conf)
	boot.Yaml("./app.yml", conf)

	redisGroup = redis.NewGroup(conf.Storage, nil)
	conf.Option.Storage = NewRedisPersonas(redisGroup, "")
	personas = NewPersonas(&conf.Option)
}

func TestPersonas_GetProperty(t *testing.T) {
	id := strconv.FormatInt(time.Now().Unix(), 10)

	//测试需要，真实情况根据自己业务而定
	defer func() {
		if _, err := personas.Destroy(id); err != nil {
			t.Fatal(err.Error())
		}
	}()

	data, err := personas.Load(id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if personas.Exists(data, 1) {
		t.Fatal("want false, got true")
	}

	ok, err := personas.SetProperty(id, 0, true)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ok {
		t.Fatal("want true, got false")
	}

	exists, err := personas.GetProperty(id, 0)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !exists {
		t.Fatal("want true, got false")
	}

	ok, err = personas.DelProperty(id, 7)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !ok {
		t.Fatal("want true, got false")
	}

	exists, err = personas.GetProperty(id, 7)
	if err != nil {
		t.Fatal(err.Error())
	}

	if exists {
		t.Fatal("want false, got true")
	}
}
