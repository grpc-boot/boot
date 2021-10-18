package hash

import (
	"crypto"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"hash/crc32"
	"strconv"
	"testing"
	"time"

	"github.com/grpc-boot/boot"
	"github.com/grpc-boot/boot/atomic"
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
	boot.CanHash
	id []byte
}

func (d *Db) HashCode() (hashValue uint32) {
	return crc32.ChecksumIEEE(d.id)
}

// go test -bench=. -benchmem -v
// BenchmarkHashRing_GetIndex-4    24148118                65.9 ns/op            16 B/op          1 allocs/op
func BenchmarkHashRing_GetIndex(b *testing.B) {
	serverList := make([]boot.CanHash, 0, len(hostList))

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

func TestMd5(t *testing.T) {
	data := []byte(time.Now().String())
	hashMd5 := Md5(data)
	t.Log(hashMd5)

	oMd5 := fmt.Sprintf("%x", md5.Sum(data))
	t.Log(oMd5)
	if hashMd5 != oMd5 {
		t.Fatal("want true, got false")
	}
	t.Log(string(data))
}

func TestSha1(t *testing.T) {
	data := []byte(time.Now().String())
	hashSha1 := Sha1(data)
	t.Log(hashSha1)

	oSha1 := fmt.Sprintf("%x", sha1.Sum(data))
	t.Log(oSha1)
	if hashSha1 != oSha1 {
		t.Fatal("want true, got false")
	}
	t.Log(string(data))
}

func TestHMac(t *testing.T) {
	data := time.Now().String()
	key := strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Log(data, key)

	hSha1 := HMac([]byte(key), []byte(data), crypto.SHA1)
	t.Log(hSha1)
}

func BenchmarkHash(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data := strconv.FormatInt(time.Now().UnixNano(), 10)
			//Md5([]byte(data))
			Hash([]byte(data), crypto.MD5)
		}
	})
}
