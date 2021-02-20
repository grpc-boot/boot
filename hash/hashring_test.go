package hash

import (
	"github.com/grpc-boot/boot/atomic"
	"hash/crc32"
	"strconv"
	"testing"
)

var (
	group    = &Group{}
	hostList = []string{
		"192.168.1.135:3551",
		"192.168.1.135:3552",
		"192.168.1.135:3553",
		"192.168.1.135:3554",
		"192.168.1.135:3555",
		"192.168.1.135:3556",
		"192.168.1.135:3557",
		"192.168.1.135:3558",
		"192.168.1.135:3559",
	}
)

type Group struct {
	ring *DefaultRing
}

type Db struct {
	CanHash
	id []byte
}

func (d *Db) HashCode() (hashValue uint32) {
	return crc32.ChecksumIEEE(d.id)
}

// go test -bench=. -benchmem -v
// BenchmarkHashRing_GetIndex-4    24148118                65.9 ns/op            16 B/op          1 allocs/op
func BenchmarkHashRing_GetIndex(b *testing.B) {
	serverList := make([]CanHash, 0, len(hostList))

	for _, server := range hostList {
		serverList = append(serverList, &Db{
			id: []byte(server),
		})
	}

	group.ring = NewDefaultRing(serverList)
	var val atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := group.ring.Get([]byte(strconv.FormatUint(val.Incr(1), 10)))
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	})
}
