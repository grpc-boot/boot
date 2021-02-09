package container

import (
	"sync/atomic"
	"unsafe"
)

type Queue interface {
	Enqueue(item interface{})
	Dequeue() (item interface{})
	Empty() bool
}

type node struct {
	value interface{}
	next  unsafe.Pointer
}

func load(p *unsafe.Pointer) (n *node) {
	return (*node)(atomic.LoadPointer(p))
}

func cas(p *unsafe.Pointer, old, new *node) bool {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}

type lockFreeQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	len  int32
}

func NewLockFreeQueue() Queue {
	n := unsafe.Pointer(&node{})
	return &lockFreeQueue{head: n, tail: n}
}

func (q *lockFreeQueue) Enqueue(item interface{}) {
	n := &node{value: item}
loop:
	tail := load(&q.tail)
	next := load(&tail.next)
	// Are tail and next consistent?
	if tail == load(&q.tail) {
		if next == nil {
			// Try to link node at the end of the linked list.
			if cas(&tail.next, next, n) {
				// Enqueue is done. Try to swing tail to the inserted node.
				cas(&q.tail, tail, n)
				atomic.AddInt32(&q.len, 1)
				return
			}
		} else { // tail was not pointing to the last node
			// Try to swing Tail to the next node.
			cas(&q.tail, tail, next)
		}
	}
	goto loop
}

func (q *lockFreeQueue) Dequeue() (item interface{}) {
loop:
	head := load(&q.head)
	tail := load(&q.tail)
	next := load(&head.next)
	// Are head, tail, and next consistent?
	if head == load(&q.head) {
		// Is queue empty or tail falling behind?
		if head == tail {
			// Is queue empty?
			if next == nil {
				return nil
			}
			cas(&q.tail, tail, next) // tail is falling behind. Try to advance it.
		} else {
			// Read value before CAS, otherwise another dequeue might free the next node.
			task := next.value
			if cas(&q.head, head, next) {
				// Dequeue is done. return value.
				atomic.AddInt32(&q.len, -1)
				return task
			}
		}
	}
	goto loop
}

func (q *lockFreeQueue) Empty() bool {
	return atomic.LoadInt32(&q.len) == 0
}
