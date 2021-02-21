package boot

import "unsafe"

type Node struct {
	Value interface{}
}

func NewNode(value interface{}) *Node {
	return &Node{Value: value}
}

type SingleNode struct {
	Value interface{}
	Right unsafe.Pointer
}

func NewSingleNode(value interface{}, right unsafe.Pointer) (node *SingleNode) {
	return &SingleNode{
		Value: value,
		Right: right,
	}
}

type DoubleNode struct {
	Value interface{}
	Left  unsafe.Pointer
	Right unsafe.Pointer
}

func NewDoubleNode(value interface{}, left unsafe.Pointer, right unsafe.Pointer) (node *DoubleNode) {
	return &DoubleNode{
		Value: value,
		Left:  left,
		Right: right,
	}
}
