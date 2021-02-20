package main

import (
	"net/http"

	"github.com/grpc-boot/boot/grace"
)

func main() {
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	})

	server, err := grace.NewTcpServer(&grace.Option{
		Addr:   ":8090",
		Server: &http.Server{},
	})

	if err != nil {
		panic(err.Error())
	}

	server.Serve()
}
