package container

import (
	atomic2 "sync/atomic"
	"unsafe"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/atomic"
)

type Queue interface {
	Push(value interface{})
	Pop() (value interface{})
	Length() (len uint32)
}

type locklessQueue struct {
	head   unsafe.Pointer
	tail   unsafe.Pointer
	length atomic.Uint32
}

func NewLocklessQueue() Queue {
	root := unsafe.Pointer(&boot.SingleNode{})
	return &locklessQueue{head: root, tail: root}
}

func (llq *locklessQueue) Push(value interface{}) {
	var (
		node = unsafe.Pointer(&boot.SingleNode{
			Value: value,
		})

		tail  *boot.SingleNode
		right *boot.SingleNode
	)

	for {
		tail = (*boot.SingleNode)(atomic2.LoadPointer(&llq.tail))
		right = (*boot.SingleNode)(atomic2.LoadPointer(&tail.Right))
		if tail == (*boot.SingleNode)(atomic2.LoadPointer(&llq.tail)) {
			if right == nil {
				//保证单向链表指针指向
				if atomic2.CompareAndSwapPointer(&tail.Right, unsafe.Pointer(right), node) {
					//①：此处失败会在②处将指针更换正确，故llq.tail不一定是真正的tail
					atomic2.CompareAndSwapPointer(&llq.tail, unsafe.Pointer(tail), node)
					llq.length.Incr(1)
					return
				}
			} else {
				//②：纠正llq.tail，①失败会产生错误的tail
				atomic2.CompareAndSwapPointer(&llq.tail, unsafe.Pointer(tail), unsafe.Pointer(right))
			}
		}
	}
}

func (llq *locklessQueue) Pop() (value interface{}) {
	var (
		head *boot.SingleNode
		tail *boot.SingleNode
		next *boot.SingleNode
	)

	for {
		head = (*boot.SingleNode)(atomic2.LoadPointer(&llq.head))
		tail = (*boot.SingleNode)(atomic2.LoadPointer(&llq.tail))
		next = (*boot.SingleNode)(atomic2.LoadPointer(&head.Right))
		if head == (*boot.SingleNode)(atomic2.LoadPointer(&llq.head)) {
			if head == tail {
				if next == nil {
					return nil
				}
				atomic2.CompareAndSwapPointer(&llq.tail, unsafe.Pointer(tail), unsafe.Pointer(next))
			} else {
				value = next.Value
				if atomic2.CompareAndSwapPointer(&llq.head, unsafe.Pointer(head), unsafe.Pointer(next)) {
					llq.length.Incr(uint32(boot.Decr))
					return value
				}
			}
		}
	}
}

func (llq *locklessQueue) Length() (len uint32) {
	return llq.length.Get()
}
