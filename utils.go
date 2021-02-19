package boot

import (
	"context"
	"os"
	"os/signal"
	"time"
)

func Wait(timeout time.Duration, shutdown func(ctx context.Context)) {
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			cleanupDone <- true
		}
	}()

	<-cleanupDone
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdown(ctx)
}
