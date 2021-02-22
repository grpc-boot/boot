package personas

import (
	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/container"
)

type Option struct {
	Max   uint16 `yaml:"max" json:"max"`
	Cache Storage
}

type Personas struct {
	max     uint16
	cache   *container.Map
	storage Storage
}

func NewPersonas(option *Option) (personas *Personas) {
	personas = &Personas{
		cache: container.NewMap(),
	}

	if option.Max == 0 {
		personas.max = boot.DefaultPersonasMax
	} else {
		personas.max = option.Max
	}

	return
}

func (p *Personas) exists(data []byte, property uint16) (exists bool) {
	return
}

func (p *Personas) LoadOrInitProperties(id string) (value []byte, err error) {
	value, err = p.storage.Load(id)
	if err != nil {
		return nil, err
	}

	if len(value) < 1 {
		value, err = p.storage.Init(id, p.max)
		if err != nil {
			return nil, err
		}
	}

	p.cache.Set(id, value)
	return value, nil
}

func (p *Personas) GetProperty(id string, property uint16) (exists bool, err error) {
	val, ok := p.cache.Get(id)
	if !ok {
		val, err = p.LoadOrInitProperties(id)
		if err != nil {
			return false, err
		}
	}

	return p.exists(val.([]byte), property), nil
}

func (p *Personas) SetProperty(id string, property uint16, value uint8) (ok bool, err error) {
	ok, err = p.storage.Set(id, property, value)
	if err != nil {
		return ok, nil
	}

	_, err = p.LoadOrInitProperties(id)
	if err != nil {
		return false, err
	}
	return true, err
}
