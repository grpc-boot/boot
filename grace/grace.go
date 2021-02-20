package grace

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	gracefulKey        = "GRACEFUL"
	gracefulListenerFd = 3
)

var (
	ErrGetListenerFdFailed = errors.New("get listener fd failed")
)

type Option struct {
	Addr   string `yaml:"addr" json:"addr"`
	Server Server
}

type Server interface {
	Serve(l net.Listener) error
	Shutdown(ctx context.Context) error
}

type TcpServer struct {
	signalChan chan os.Signal
	clearChan  chan bool
	listener   net.Listener
	server     Server
}

func NewTcpServer(option *Option) (server *TcpServer, err error) {
	var listener net.Listener
	listener, err = net.Listen("tcp", option.Addr)
	if err != nil {
		return nil, err
	}

	server = &TcpServer{
		signalChan: make(chan os.Signal, 1),
		clearChan:  make(chan bool, 1),
		listener:   listener,
		server:     option.Server,
	}
	return
}

func (ts *TcpServer) Listener() net.Listener {
	return ts.listener
}

func (ts *TcpServer) startServe(l net.Listener) {
	ts.listener = l
	go func() {
		if err := ts.server.Serve(ts.listener); err != nil {
			log.Println("error:", err.Error())
		}
	}()
}

func (ts *TcpServer) shutdown() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return ts.server.Shutdown(ctx)
}

func (ts *TcpServer) Serve(envList []string) {
	ts.startServe(ts.listener)
	log.Println("start")

	go func() {
		for {
			signal.Notify(ts.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			sig := <-ts.signalChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				fmt.Println("INT")
				ts.clearChan <- true
			case syscall.SIGHUP:
				fmt.Println("HUP")
				listener, _, err := Fork(envList)
				if err != nil {
					log.Println("error:", err.Error())
					continue
				}

				ts.startServe(listener)
				log.Println("grace")

				err = StopOldProcess()
				if err != nil {
					log.Println("error:", err.Error())
				}
			}
		}
	}()
	fmt.Println("clear")
	<-ts.clearChan
	if err := ts.shutdown(); err != nil {
		log.Println("error:", err.Error())
	}
}

func (ts *TcpServer) Wait() {
	cleanupDone := make(chan bool)
	go func() {
		for {
			signal.Notify(ts.signalChan, syscall.SIGINT, syscall.SIGTERM)
			sig := <-ts.signalChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				cleanupDone <- true
			}
		}
	}()
	<-cleanupDone
	if err := ts.shutdown(); err != nil {
		log.Println("error:", err.Error())
	}
}

func Fork(envList []string) (listener net.Listener, process *os.Process, err error) {
	file := os.NewFile(gracefulListenerFd, "")
	listener, err = net.FileListener(file)
	if err != nil {
		return nil, nil, ErrGetListenerFdFailed
	}

	var listenerFd *os.File
	listenerFd, err = listener.(*net.TCPListener).File()
	if err != nil {
		return nil, nil, ErrGetListenerFdFailed
	}

	procAtt := &os.ProcAttr{
		Files: []*os.File{
			os.Stdin,
			os.Stdout,
			os.Stderr,
			listenerFd,
		},
	}

	if len(envList) > 0 {
		procAtt.Env = envList
	}

	argv := make([]string, 0)
	for _, arg := range os.Args {
		if arg == gracefulKey {
			break
		}
		argv = append(argv, arg)
	}
	argv = append(argv, gracefulKey)

	process, err = os.StartProcess(os.Args[0], argv, procAtt)
	return
}

func StopOldProcess() (err error) {
	process, err := os.FindProcess(os.Getppid())
	if err != nil {
		return err
	}

	return process.Signal(syscall.SIGTERM)
}
