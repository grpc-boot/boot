package atomic

import "sync/atomic"

type Acquire struct {
	value uint32
}

func (a *Acquire) Acquire() (acquired bool) {
	if val := atomic.LoadUint32(&a.value); val == 0 {
		return atomic.CompareAndSwapUint32(&a.value, 0, 1)
	}
	return false
}

func (a *Acquire) IsRelease() (release bool) {
	return atomic.LoadUint32(&a.value) == 0
}

func (a *Acquire) Release() {
	atomic.StoreUint32(&a.value, 0)
}
