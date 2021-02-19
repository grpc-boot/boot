package grace

import (
	"context"
	"errors"
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

type Server struct {
	signalChan chan os.Signal
	listener   net.Listener
	port       string
}

func NewServer(port string) (server *Server, err error) {
	var listener net.Listener
	listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}

	server = &Server{
		signalChan: make(chan os.Signal, 1),
		listener:   listener,
		port:       port,
	}
	return
}

func (s *Server) Listener() net.Listener {
	return s.listener
}

func (s *Server) Serve(envList []string, timeout time.Duration, shutdown func(ctx context.Context)) {
	cleanupDone := make(chan bool)
	go func() {
		for {
			signal.Notify(s.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			sig := <-s.signalChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				cleanupDone <- true
			case syscall.SIGHUP:
				listener, _, err := Fork(envList)
				if err != nil {
					log.Println("error:", err.Error())
					continue
				}
				s.listener = listener
				err = StopOldProcess()
				if err != nil {
					log.Println("error:", err.Error())
				}
			}
		}
	}()
	<-cleanupDone
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdown(ctx)
}

func (s *Server) Wait(timeout time.Duration, shutdown func(ctx context.Context)) {
	cleanupDone := make(chan bool)
	go func() {
		for {
			signal.Notify(s.signalChan, syscall.SIGINT, syscall.SIGTERM)
			sig := <-s.signalChan
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				cleanupDone <- true
			}
		}
	}()
	<-cleanupDone
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdown(ctx)
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
