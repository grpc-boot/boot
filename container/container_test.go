package container

import (
	"hash/crc32"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/atomic"
)

var (
	m *Map
)

type Data struct {
	boot.CanHash

	id string
}

func (d *Data) HashCode() (hashValue uint32) {
	return crc32.ChecksumIEEE([]byte(d.id))
}

func init() {
	m = NewMap()
}

func TestMap(t *testing.T) {
	d := Data{
		CanHash: nil,
		id:      "cc",
	}

	keyValue := map[interface{}]interface{}{
		"user": map[string]interface{}{
			"id":   15,
			"name": "ddadf",
		},
		"listLength": 34,
		"key":        "value",
		d:            55,
	}

	for key, value := range keyValue {
		m.Set(key, value)
	}

	if uint64(len(keyValue)) != m.Length() {
		t.Fatalf("want %d, got %d", len(keyValue), m.Length())
	}

	val, exists := m.Get("user")
	if !exists {
		t.Fatalf("want true, got %t", exists)
	}

	if _, ok := val.(map[string]interface{}); !ok {
		t.Fatalf("want true, got %t", ok)
	}

	val, exists = m.Get(d)
	if !exists {
		t.Fatalf("want true, got %t", exists)
	}

	if val != 55 {
		t.Fatal("want true, got false")
	}

	m.Delete("key")

	if exists = m.Exists("key"); exists {
		t.Fatalf("want false, got %t", exists)
	}

	if uint64(len(keyValue)-1) != m.Length() {
		t.Fatalf("want %d, got %d", len(keyValue)-1, m.Length())
	}
}

func TestSet(t *testing.T) {
	hs := NewSet()

	list := []interface{}{
		"12",
		12,
		int64(12),
	}
	for _, val := range list {
		hs.Add(val)
	}

	if hs.Size() != len(list) {
		t.Fatalf("want %d, got %d", len(list), hs.Size())
	}

	//int 12已经存在
	hs.Add(12)
	if hs.Size() != len(list) {
		t.Fatalf("want %d, got %d", len(list), hs.Size())
	}

	hs.Remove(12)
	if hs.Contains(12) {
		t.Fatalf("want false, got %t", hs.Contains(12))
	}

	if !hs.Contains(int64(12)) {
		t.Fatalf("want true, got %t", hs.Contains(int64(12)))
	}
}

// BenchmarkMap_SetParallel-4       3692262               330 ns/op              49 B/op          2 allocs/op
func BenchmarkMap_SetParallel(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set(time.Now().UnixNano(), time.Now().UnixNano())
		}
	})
}

// BenchmarkMap_Set-4               2544157               641 ns/op             143 B/op          2 allocs/op
func BenchmarkMap_Set(b *testing.B) {
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m.Set(time.Now().UnixNano(), time.Now().UnixNano())
	}
}

// BenchmarkMap_GetParallel-4      14161170                95.0 ns/op             8 B/op          1 allocs/op
func BenchmarkMap_GetParallel(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Get(time.Now().UnixNano())
		}
	})
}

// BenchmarkMap_Get-4               5327881               225 ns/op               8 B/op          1 allocs/op
func BenchmarkMap_Get(b *testing.B) {
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		m.Get(time.Now().UnixNano())
	}
}

func BenchmarkLocklessQueue_Push(b *testing.B) {
	queue := NewLocklessQueue()
	var val atomic.Uint64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			queue.Push(val.Incr(1))
		}
	})
}

func BenchmarkChain_Next(b *testing.B) {
	type Response struct {
		Status int64
		Msg    string
		Info   struct {
			Id   int64
			Name string
		}
	}

	chain := NewChain()

	var count atomic.Uint64
	chain.Use(func(value interface{}) (handled bool) {
		value.(*Response).Info.Id = rand.Int63n(math.MaxUint32)
		return
	})

	chain.Use(func(value interface{}) (handled bool) {
		if value.(*Response).Info.Id&math.MaxUint8 <= math.MaxInt8 {
			value.(*Response).Status = 200
			value.(*Response).Info.Name = "ok"
			handled = true
		}
		return
	})

	chain.Use(func(value interface{}) (handled bool) {
		value.(*Response).Status = 401
		val := count.Incr(1)
		if val&math.MaxUint8 == 0 {
			go func(r *Response) {
				r.Status = 500
			}(value.(*Response))
		}
		return
	})

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp := &Response{
				Status: 100 + rand.Int63n(401),
				Msg:    "dsadf",
			}
			chain.Next(resp)
		}
	})
}
