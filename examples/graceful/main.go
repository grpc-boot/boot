package main

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/grpc-boot/boot/grace"
)

type HttpServer struct {
	grace.Server

	server *http.Server
}

func (hs *HttpServer) Serve(l net.Listener) error {
	return hs.server.Serve(l)
}

func (hs *HttpServer) Shutdown(ctx context.Context) error {
	fmt.Println("shutdown")
	return hs.server.Shutdown(ctx)
}

func main() {
	fmt.Println("begin")

	httpServer := &HttpServer{
		server: &http.Server{},
	}

	server, err := grace.NewTcpServer(&grace.Option{
		Addr:   ":8090",
		Server: httpServer,
	})

	if err != nil {
		panic(err.Error())
	}

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Println(request.RequestURI)
		_, _ = writer.Write([]byte("ok"))
	})

	server.Serve(nil)
}
