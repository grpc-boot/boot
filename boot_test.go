package boot

import (
	"math/rand"
	"testing"
)

func TestIp2Long(t *testing.T) {
	caseList := []string{
		"256.12.12.12",
		"25.12.12..12",
		".25.12.12.12",
		"1233.12.12.12",
		"26.a.12.12",
		"56.12.32",
		"56.12.34.45.32",
		"127.0.0.1",
		"21.35.116.73",
	}

	for _, ip := range caseList {
		ipVal, err := Ip2Long(ip)
		t.Log(ip, ipVal, err)
		if err == nil {
			t.Log(Long2Ip(ipVal))
		}
	}
}

func BenchmarkIp2Long(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ip := Long2Ip(rand.Uint32())
			Ip2Long(ip)
		}
	})
}

func BenchmarkLong2Ip(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Long2Ip(rand.Uint32())
		}
	})
}
