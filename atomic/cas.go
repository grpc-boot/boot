package atomic

import (
	"sync/atomic"
	"unsafe"

	"github.com/grpc-boot/boot"
)

func Get(p *unsafe.Pointer) (n *boot.Node) {
	return (*boot.Node)(atomic.LoadPointer(p))
}

func SetValue(p *unsafe.Pointer, value interface{}) {
	node := boot.NewNode(value)
	atomic.StorePointer(p, unsafe.Pointer(node))
}

func Set(p *unsafe.Pointer, node *boot.Node) {
	atomic.StorePointer(p, unsafe.Pointer(node))
}

func Cas(p *unsafe.Pointer, old, new *boot.Node) bool {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}
