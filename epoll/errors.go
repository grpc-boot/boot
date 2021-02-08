package epoll

import "errors"

var (
	ErrUnsupportedProtocol = errors.New("only tcp/tcp4/tcp6/unix supported")
)
