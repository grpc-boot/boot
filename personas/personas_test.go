package personas

import (
	"fmt"
	"github.com/grpc-boot/boot"
	"strconv"
	"testing"
	"time"

	"github.com/grpc-boot/boot/redis"
)

var (
	conf       *Conf
	redisGroup *redis.Group
	personas   *Personas
)

type Conf struct {
	Option     Option       `yaml:"option" json:"option"`
	RedisGroup redis.Option `yaml:"redis" json:"redis"`
}

func init() {
	conf = new(Conf)
	boot.Yaml("./app.yml", conf)

	redisGroup = redis.NewGroup([]redis.Option{conf.RedisGroup}, nil)
	conf.Option.Storage = NewRedisPersonas(redisGroup, "")
	personas = NewPersonas(&conf.Option)
}

func TestPersonas_Exists(t *testing.T) {
	id := strconv.FormatInt(time.Now().Unix(), 10)
	val, err := personas.LoadProperties(id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if personas.Exists(val, 1) {
		t.Fatal("want false, got true")
	}

	value, err := personas.SetProperty(id, 0, 1)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !personas.Exists(value, 0) {
		t.Fatal("want true, got false")
	}

	exists, err := personas.GetProperty(id, 0)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !exists {
		t.Fatal("want true, got false")
	}

	value, err = personas.SetProperty(id, 7, 1)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !personas.Exists(value, 7) {
		t.Fatal("want true, got false")
	}

	value, err = personas.SetProperty(id, 8, 1)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !personas.Exists(value, 8) {
		t.Fatal("want true, got false")
	}

	value, err = personas.SetProperty(id, 16, 1)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !personas.Exists(value, 16) {
		t.Fatal("want true, got false")
	}

	out := ""
	for _, v := range value {
		out += fmt.Sprintf("%b", v)
	}
	t.Fatal(out)
}
