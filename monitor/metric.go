package monitor

import "github.com/grpc-boot/boot/atomic"

type Metric struct {
	name  string
	value atomic.Uint64
}

func (m *Metric) AddInt64(val int64) (newValue uint64) {
	return m.value.Incr(uint64(val))
}

func (m *Metric) Add(val uint64) (newValue uint64) {
	return m.value.Incr(val)
}

func (m *Metric) Set(val uint64) {
	m.value.Set(val)
}

func (m *Metric) Get() (val uint64) {
	return m.value.Get()
}
