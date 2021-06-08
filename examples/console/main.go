package main

import (
	"github.com/grpc-boot/boot/console"
	"time"
)

func main() {
	console.Black("%s", time.Now().String())
	console.Red("%s", time.Now().String())
	console.Yellow("%s", time.Now().String())
	console.Green("%s", time.Now().String())
	console.Blue("%s", time.Now().String())
	console.Fuchsia("%s", time.Now().String())
}
