package epoll

import "github.com/grpc-boot/boot/container"

const (
	EventRead  = 1 << 1
	EventWrite = 1 << 2
)

type EventHandler func(fd int, event uint8)

type EventLoop interface {
	Start() (err error)
	Accept(fd int) (err error)
	Read(conn *Connection) (err error)
	Write(conn *Connection) (err error)
}

type eventLoop struct {
	EventLoop

	poller      *Epoll
	connections *container.Map
}

func (el *eventLoop) Accept(fd int) (err error) {
	if err = el.poller.AddRead(fd); err != nil {
		return err
	}

	conn := newConnection(fd)
	el.connections.Set(fd, conn)

	return nil
}

func (el *eventLoop) Read(conn *Connection) (err error) {
	conn.Access()
	return nil
}

func (el *eventLoop) Write(conn *Connection) (err error) {
	return nil
}

func (el *eventLoop) Handler(fd int, event uint8) (err error) {
	if conn, exists := el.connections.Get(fd); exists {
		if event&EventRead == EventRead {
			if err = el.Read(conn.(*Connection)); err != nil {
				return err
			}
		}

		if event&EventWrite == EventWrite {
			if err = el.Write(conn.(*Connection)); err != nil {
				return err
			}
		}

		return nil
	}
	return el.Accept(fd)
}
