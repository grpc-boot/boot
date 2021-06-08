package atomic

import (
	"testing"
	"unsafe"

	"github.com/grpc-boot/boot"
)

type User struct {
	Id int64
}

func TestCas(t *testing.T) {
	var data unsafe.Pointer

	user1 := boot.NewNode(User{Id: 1})

	Set(&data, user1)

	user2 := boot.NewNode(User{
		Id: 2,
	})

	if !Cas(&data, user1, user2) {
		t.Fatal("want true, got false")
	}

	data1 := boot.NewNode(3)
	if !Cas(&data, user2, data1) {
		t.Fatal("want true, got false")
	}

	SetValue(&data, 4)
	if Get(&data).Value != 4 {
		t.Fatal("want true, got false")
	}
}

func TestBool_Get(t *testing.T) {
	var run Bool

	if run.Get() {
		t.Fatal("want true, got false")
	}

	run.Set(true)

	if !run.Get() {
		t.Fatal("want true, got false")
	}
}

func TestAcquire_Acquire(t *testing.T) {
	var lock Acquire

	if !lock.Acquire() {
		t.Fatal("want true, got false")
	}

	lock.Release()

	if !lock.IsRelease() {
		t.Fatal("want false, got true")
	}
}
