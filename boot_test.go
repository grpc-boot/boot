package boot

import (
	"math/rand"
	"testing"
)

func TestReleaseArgs(t *testing.T) {
	args := AcquireArgs()

	args = append(args, 5, 6)
	t.Log(args)

	ReleaseArgs(&args)

	t.Fatal(args)
}

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
	t.Fatal("")
}

// cpu: Intel(R) Core(TM) i5-8257U CPU @ 1.40GHz
// go test -bench=. -benchmem BenchmarkIp2Long-8       8377591               141.3 ns/op             0 B/op          0 allocs/op
func BenchmarkIp2Long(b *testing.B) {
	ipList := make([]string, 0, 40960002)
	for i := 0; i < 40960000; i++ {
		ipList = append(ipList, Long2Ip(rand.Uint32()))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Ip2Long(ipList[i])
	}
}

// cpu: Intel(R) Core(TM) i5-8257U CPU @ 1.40GHz
// go test -bench=. -benchmem BenchmarkLong2Ip-8       6062264               199.9 ns/op            15 B/op          1 allocs/op
func BenchmarkLong2Ip(b *testing.B) {
	ipList := make([]uint32, 0, 40960002)
	for i := 0; i < 40960000; i++ {
		ipList = append(ipList, rand.Uint32())
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Long2Ip(ipList[i])
	}
}
