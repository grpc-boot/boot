package atomic

import (
	"sync/atomic"
)

type Bool struct {
	value uint32
}

func (b *Bool) Set(value bool) {
	if value {
		atomic.StoreUint32(&b.value, 1)
		return
	}
	atomic.StoreUint32(&b.value, 0)
}

func (b *Bool) Get() (val bool) {
	return atomic.LoadUint32(&b.value) == 1
}
