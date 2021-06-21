package personas

import (
	"github.com/grpc-boot/boot"
)

type Option struct {
	Max     uint16 `yaml:"max" json:"max"`
	Storage Storage
}

type Storage interface {
	Load(id string) (data []byte, err error)
	Set(id string, property uint16, value bool) (ok bool, err error)
	Get(id string, property uint16) (exists bool, err error)
	Destroy(id string) (ok bool, err error)
}

type Personas struct {
	max     uint16
	storage Storage
}

func NewPersonas(option *Option) (personas *Personas) {
	personas = &Personas{
		storage: option.Storage,
	}

	if option.Max == 0 {
		personas.max = boot.DefaultPersonasMax
	} else {
		personas.max = option.Max
	}
	return personas
}

func (p *Personas) Exists(data []byte, property uint16) (exists bool) {
	if data == nil {
		return false
	}

	index := property / 8
	if len(data) < int(index) {
		return false
	}

	val := uint8(1 << (7 - (property % 8)))
	return (data)[index]&val > 0
}

func (p *Personas) Load(id string) (data []byte, err error) {
	return p.storage.Load(id)
}

func (p *Personas) SetProperty(id string, property uint16, value bool) (ok bool, err error) {
	return p.storage.Set(id, property, value)
}

func (p *Personas) GetProperty(id string, property uint16) (exists bool, err error) {
	return p.storage.Get(id, property)
}

func (p *Personas) DelProperty(id string, property uint16) (ok bool, err error) {
	return p.storage.Set(id, property, false)
}

func (p *Personas) Destroy(id string) (ok bool, err error) {
	return p.storage.Destroy(id)
}
