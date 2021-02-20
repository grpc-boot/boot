package grace

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

type Hold struct {
	signalChan chan os.Signal
	shutdown   func(ctx context.Context) (err error)
}

func NewHold(shutdown func(ctx context.Context) (err error)) *Hold {
	if shutdown == nil {
		panic("param shutdown must be gave")
	}

	return &Hold{
		signalChan: make(chan os.Signal, 1),
		shutdown:   shutdown,
	}
}

func (h *Hold) Start() {
	for {
		signal.Notify(h.signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGUSR2)
		sig := <-h.signalChan
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			_ = h.shutdown(ctx)
			signal.Stop(h.signalChan)
			cancel()
			return
		case syscall.SIGHUP, syscall.SIGUSR2:
			if err := h.reload(); err != nil {
				log.Println("error:", err.Error())
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			_ = h.shutdown(ctx)
			cancel()
			return
		}
	}
}

func (h *Hold) reload() (err error) {
	args := []string{"-graceful"}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Start()
}
