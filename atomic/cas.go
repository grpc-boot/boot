package atomic

import (
	"sync/atomic"
	"unsafe"
)

type Node struct {
	value interface{}
}

func NewNode(value interface{}) *Node {
	return &Node{value: value}
}

func Get(p *unsafe.Pointer) (n *Node) {
	return (*Node)(atomic.LoadPointer(p))
}

func SetValue(p *unsafe.Pointer, value interface{}) {
	node := NewNode(value)
	atomic.StorePointer(p, unsafe.Pointer(node))
}

func Set(p *unsafe.Pointer, node *Node) {
	atomic.StorePointer(p, unsafe.Pointer(node))
}

func Cas(p *unsafe.Pointer, old, new *Node) bool {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}
