package main

import (
	"log"
	"os"
	"time"
)

var (
	logFile = "./runtime.log"
)

func main() {
	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err.Error())
	}

	log.SetOutput(file)

	log.Printf("%s", time.Now().String())
}
