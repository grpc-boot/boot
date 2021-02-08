package epoll

import (
	"golang.org/x/sys/unix"
	"os"
	"runtime"
	"sync/atomic"
)

const (
	epollRead      = unix.EPOLLIN | unix.EPOLLPRI
	epollWrite     = unix.EPOLLOUT
	epollReadWrite = epollRead | epollWrite
)

const (
	errorEvent = unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLRDHUP
	readEvent  = errorEvent | unix.EPOLLIN | unix.EPOLLPRI
	writeEvent = errorEvent | unix.EPOLLOUT
)

type Epoll struct {
	fd      int
	eventFd int
	status  uint32
	done    chan struct{}
}

func NewEpoll() (epoll *Epoll, err error) {
	epoll = &Epoll{}

	if epoll.fd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		epoll.close()
		return nil, os.NewSyscallError("epoll_create1", err)
	}

	if epoll.eventFd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		epoll.close()
		return nil, os.NewSyscallError("eventfd", err)
	}

	if err = epoll.AddRead(epoll.eventFd); err != nil {
		epoll.close()
		return nil, err
	}

	epoll.done = make(chan struct{})
	return
}

func (e *Epoll) close() {
	if e.fd > 0 {
		_ = unix.Close(e.fd)
	}

	if e.eventFd > 0 {
		_ = unix.Close(e.eventFd)
	}
}

func (e *Epoll) AddRead(fd int) error {
	return os.NewSyscallError("epoll_ctl add read", unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: epollRead,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) AddWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl add write", unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: epollWrite,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) AddReadWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl add readWrite", unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: epollReadWrite,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) ModRead(fd int) error {
	return os.NewSyscallError("epoll_ctl mod read", unix.EpollCtl(e.fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: epollRead,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) ModWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl mod read", unix.EpollCtl(e.fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: epollWrite,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) ModReadWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl mod readWrite", unix.EpollCtl(e.fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: epollReadWrite,
		Fd:     int32(fd),
	}))
}

func (e *Epoll) Remove(fd int) error {
	return os.NewSyscallError("epoll_ctl del", unix.EpollCtl(e.fd, unix.EPOLL_CTL_DEL, fd, nil))
}

func (e *Epoll) Stop() {
	if atomic.LoadUint32(&e.status) == 0 {
		return
	}

	atomic.StoreUint32(&e.status, 0)
	<-e.done
	e.close()
	return
}

func (e *Epoll) Poll(handler EventHandler) {
	defer close(e.done)

	events := make([]unix.EpollEvent, 128)
	atomic.StoreUint32(&e.status, 1)

	for {
		n, err := unix.EpollWait(e.fd, events, -1)
		if n == 0 || (n < 0 && err != unix.EINTR) {
			runtime.Gosched()
			continue
		}

		for i := 0; i < n; i++ {
			if fd := int(events[i].Fd); fd != e.eventFd {
				var rEvents uint8
				if events[i].Events&readEvent != 0 {
					rEvents |= EventRead
				}

				if events[i].Events&writeEvent != 0 {
					rEvents |= EventWrite
				}

				handler(fd, rEvents)
			} else {

			}
		}

		if atomic.LoadUint32(&e.status) == 0 {
			handler(-1, 0)
			return
		}
	}
}
