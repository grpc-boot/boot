package personas

import (
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/container"
)

type Option struct {
	Max          uint16 `yaml:"max" json:"max"`
	UseCache     bool   `yaml:"useCache" json:"useCache"`
	CacheTimeout int64  `yaml:"cacheTimeout" json:"cacheTimeout"`
	Storage      Storage
}

type item struct {
	value    []byte
	expireAt int64
}

type Personas struct {
	max          uint16
	cache        *container.Map
	useCache     bool
	cacheTimeout int64
	storage      Storage
}

func NewPersonas(option *Option) (personas *Personas) {
	personas = &Personas{
		useCache:     option.UseCache,
		cacheTimeout: option.CacheTimeout,
		storage:      option.Storage,
	}

	if personas.useCache {
		if personas.cacheTimeout < 10 {
			personas.cacheTimeout = 10
		}

		personas.cache = container.NewMap()
	}

	if option.Max == 0 {
		personas.max = boot.DefaultPersonasMax
	} else {
		personas.max = option.Max
	}
	return personas
}

func (p *Personas) Exists(value []byte, property uint16) (exists bool) {
	index := property / 8
	if len(value) < int(index) {
		return false
	}

	return value[index]&byte(property%8) > 0
}

func (p *Personas) reloadProperties(id string) (value []byte, err error) {
	value, err = p.storage.Load(id)
	if err != nil {
		return nil, err
	}

	if len(value) < 1 {
		value, err = p.storage.Set(id, p.max, 0)
		if err != nil {
			return nil, err
		}
	}

	if p.useCache {
		p.cache.Set(id, item{
			value:    value,
			expireAt: time.Now().Unix() + p.cacheTimeout,
		})
	}

	return value, nil
}

func (p *Personas) LoadProperties(id string) (value []byte, err error) {
	if p.useCache {
		i, exists := p.cache.Get(id)
		if exists {
			if it, ok := i.(item); ok {
				if it.expireAt > time.Now().Unix() {
					return it.value, nil
				}
			}
		}
	}
	return p.reloadProperties(id)
}

func (p *Personas) GetProperty(id string, property uint16) (exists bool, err error) {
	var val []byte
	val, err = p.LoadProperties(id)
	if err != nil {
		return false, err
	}

	return p.Exists(val, property), nil
}

func (p *Personas) SetProperty(id string, property uint16, value uint8) (ok bool, err error) {
	var data []byte
	data, err = p.storage.Set(id, property, value)
	if err != nil {
		return ok, nil
	}

	_, err = p.reloadProperties(id)
	if err != nil {
		return false, err
	}
	return true, err
}
