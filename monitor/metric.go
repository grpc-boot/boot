package monitor

import "github.com/grpc-boot/boot/atomic"

type Metric struct {
	name  string
	value atomic.Uint64
}

func (m *Metric) Add(val uint64) {
	m.value.Incr(val)
}

func (m *Metric) Set(val uint64) {
	m.value.Set(val)
}

func (m *Metric) Get() (val uint64) {
	return m.value.Get()
}
