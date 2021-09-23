package main

import (
	"fmt"
	snow_flake "github.com/grpc-boot/boot/snow-flake"
	"sync"
	"time"
)

var (
	sf  *snow_flake.SnowFlake
	err error

	cstSh, _ = time.LoadLocation("Asia/Shanghai")
)

func init() {
	begin := time.Date(2021, 1, 1, 0, 0, 0, 0, cstSh).UnixNano() / 1e6
	sf, err = snow_flake.New(snow_flake.ModeWait, 1, begin)
}

func main() {
	var wa sync.WaitGroup
	wa.Add(0xfff)

	for i := 0; i < 0xfff; i++ {
		go func() {
			id, _ := sf.Id(3)
			timeStamp, machineId, logicId, index := sf.Info(id)
			fmt.Printf("id:%d timestamp:%d, mcid:%d, logicId:%d, index:%d\n", id, timeStamp, machineId, logicId, index)
			wa.Done()
		}()
	}

	wa.Wait()
}
