package snow_flake

import (
	"math/rand"
	"testing"
	"time"
)

func TestSnowFlake_Id(t *testing.T) {
	begin := time.Date(2021, 1, 1, 0, 0, 0, 0, time.Local).UnixNano() / 1e6
	sf, err := New(ModeWait, 2, begin)
	if err != nil {
		t.Fatal(err)
	}

	id, err := sf.Id(0)
	timeStamp, machinId, logicId, index := sf.Info(id)
	t.Logf("%064b\n %d %d %d %d", id, timeStamp, machinId, logicId, index)

	id, err = sf.Id(1)
	timeStamp, machinId, logicId, index = sf.Info(id)
	t.Logf("%064b\n %d %d %d %d", id, timeStamp, machinId, logicId, index)

	id, err = sf.Id(2)
	timeStamp, machinId, logicId, index = sf.Info(id)
	t.Logf("%064b\n %d %d %d %d", id, timeStamp, machinId, logicId, index)

	id, err = sf.Id(3)
	timeStamp, machinId, logicId, index = sf.Info(id)
	t.Logf("%064b\n %d %d %d %d", id, timeStamp, machinId, logicId, index)
}

func BenchmarkSnowFlake_Id(b *testing.B) {
	begin := time.Date(2021, 1, 1, 0, 0, 0, 0, time.Local).UnixNano() / 1e6
	sf, err := New(ModeWait, uint8(rand.Intn(0xff)), begin)
	if err != nil {
		b.Fatal(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sf.Id(2)
		}
	})
}
