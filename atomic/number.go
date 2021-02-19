package atomic

import "sync/atomic"

type Uint64 struct {
	value uint64
}

func (u64 *Uint64) Set(value uint64) {
	atomic.AddUint64(&u64.value, value)
}

func (u64 *Uint64) Get() (value uint64) {
	return atomic.LoadUint64(&u64.value)
}

func (u64 *Uint64) Incr(value uint64) (newValue uint64) {
	return atomic.AddUint64(&u64.value, value)
}

func (u64 *Uint64) Cas(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(&u64.value, old, new)
}

type Uint32 struct {
	value uint32
}

func (u32 *Uint32) Set(value uint32) {
	atomic.AddUint32(&u32.value, value)
}

func (u32 *Uint32) Get() (value uint32) {
	return atomic.LoadUint32(&u32.value)
}

func (u32 *Uint32) Incr(value uint32) (newValue uint32) {
	return atomic.AddUint32(&u32.value, value)
}

func (u32 *Uint32) Cas(old, new uint32) bool {
	return atomic.CompareAndSwapUint32(&u32.value, old, new)
}

type Int64 struct {
	value int64
}

func (i64 *Int64) Set(value int64) {
	atomic.AddInt64(&i64.value, value)
}

func (i64 *Int64) Get() (value int64) {
	return atomic.LoadInt64(&i64.value)
}

func (i64 *Int64) Incr(value int64) (newValue int64) {
	return atomic.AddInt64(&i64.value, value)
}

func (i64 *Int64) Cas(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&i64.value, old, new)
}

type Int32 struct {
	value int32
}

func (i32 *Int32) Set(value int32) {
	atomic.AddInt32(&i32.value, value)
}

func (i32 *Int32) Get() (value int32) {
	return atomic.LoadInt32(&i32.value)
}

func (i32 *Int32) Incr(value int32) (newValue int32) {
	return atomic.AddInt32(&i32.value, value)
}

func (i32 *Int32) Cas(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&i32.value, old, new)
}
