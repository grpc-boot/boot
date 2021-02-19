package main

import (
	"context"
	"fmt"
	"github.com/grpc-boot/boot/grace"
	"net/http"
	"time"
)

func main() {
	server, err := grace.NewServer("8090")
	if err != nil {
		panic(err.Error())
	}

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Println(request.RequestURI)
		_, _ = writer.Write([]byte("ok"))
	})

	go func() {
		http.Serve(server.Listener(), nil)
	}()

	server.Serve(nil, time.Second*5, func(ctx context.Context) {
		fmt.Println("shutdown")
	})
}
