package grace

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

type TcpServer struct {
	signalChan chan os.Signal
	listener   net.Listener
	server     Server
}

func NewTcpServer(option *Option) (server *TcpServer, err error) {
	grace := flag.Bool("graceful", false, "listen on fd open 3 (internal use only)")
	flag.Parse()

	var listener net.Listener
	if *grace {
		file := os.NewFile(gracefulListenerFd, "")
		listener, err = net.FileListener(file)
		if err != nil {
			return nil, ErrGetListenerFdFailed
		}
	} else {
		listener, err = net.Listen("tcp", option.Addr)
		if err != nil {
			return nil, err
		}
	}

	server = &TcpServer{
		signalChan: make(chan os.Signal, 1),
		listener:   listener,
		server:     option.Server,
	}
	return
}

func (ts *TcpServer) Serve() {
	go func() {
		if err := ts.server.Serve(ts.listener); err != nil {
			log.Println("error:", err.Error())
		}
	}()

	for {
		signal.Notify(ts.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGUSR2)
		sig := <-ts.signalChan
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			_ = ts.shutdown()
			signal.Stop(ts.signalChan)
			return
		case syscall.SIGHUP, syscall.SIGUSR2:
			if err := ts.reload(); err != nil {
				log.Println("error:", err.Error())
				continue
			}

			_ = ts.shutdown()
			return
		}
	}
}

func (ts *TcpServer) reload() (err error) {
	listenerFd, err := ts.listener.(*net.TCPListener).File()
	if err != nil {
		return ErrGetListenerFdFailed
	}

	args := []string{"-graceful"}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{listenerFd}
	return cmd.Start()
}

func (ts *TcpServer) shutdown() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return ts.server.Shutdown(ctx)
}
