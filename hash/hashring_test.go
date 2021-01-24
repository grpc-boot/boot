package hash

import (
	"hash/crc32"
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
	ring *HashRing
}

type Db struct {
	Server
	id []byte
}

func (d *Db) HashCode() (hashValue uint32) {
	return crc32.ChecksumIEEE(d.id)
}

func BenchmarkHashRing_GetIndex(b *testing.B) {
	serverList := make([]Server, 0, len(hostList))

	for _, server := range hostList {
		serverList = append(serverList, &Db{
			id: []byte(server),
		})
	}

	group.ring = NewHashRing(serverList)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := group.ring.GetIndex([]byte("Hello World"))
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	})
}
