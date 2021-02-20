package grace

import (
	"context"
	"errors"
	"net"
)

const (
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
