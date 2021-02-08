package epoll

import (
	"golang.org/x/sys/unix"
	"net"
	"os"
)

type Listener interface {
	Listen() (err error)
}

type tcpListener struct {
	network   string
	addr      string
	reuseport bool
	fd        int
	netAdd    net.Addr
}

func TcpListener(addr string, reuseport bool) (l *tcpListener, err error) {
	l = &tcpListener{
		addr:      addr,
		reuseport: reuseport,
	}
	return
}

func (l *tcpListener) Listen() (err error) {
	var (
		family   int
		sockaddr unix.Sockaddr
	)

	if sockaddr, family, l.netAdd, err = l.getSockaddr(); err != nil {
		return
	}

	if l.fd, err = unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP); err != nil {
		err = os.NewSyscallError("socket", err)
		return
	}

	defer func() {
		if err != nil {
			_ = unix.Close(l.fd)
		}
	}()

	if err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(l.fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)); err != nil {
		return
	}

	if l.reuseport {
		if err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(l.fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)); err != nil {
			return
		}
	}

	if err = os.NewSyscallError("bind", unix.Bind(l.fd, sockaddr)); err != nil {
		return
	}

	err = os.NewSyscallError("listen", unix.Listen(l.fd, unix.SOMAXCONN))

	return
}

func (l *tcpListener) getSockaddr() (sockaddr unix.Sockaddr, family int, tcpAddr *net.TCPAddr, err error) {
	tcpAddr, err = net.ResolveTCPAddr("", l.addr)
	if err != nil {
		return
	}

	if tcpAddr.IP.To4() != nil {
		sa4 := &unix.SockaddrInet4{Port: tcpAddr.Port}
		if tcpAddr.IP != nil {
			if len(tcpAddr.IP) == 16 {
				copy(sa4.Addr[:], tcpAddr.IP[12:16])
			} else {
				copy(sa4.Addr[:], tcpAddr.IP)
			}
		}

		sockaddr, family = sa4, unix.AF_INET
		return
	}

	if tcpAddr.IP.To16() != nil {
		sa6 := &unix.SockaddrInet6{Port: tcpAddr.Port}
		if tcpAddr.IP != nil {
			copy(sa6.Addr[:], tcpAddr.IP)
		}

		if tcpAddr.Zone != "" {
			var netInterface *net.Interface
			netInterface, err = net.InterfaceByName(tcpAddr.Zone)
			if err != nil {
				return
			}

			sa6.ZoneId = uint32(netInterface.Index)
		}

		sockaddr, family = sa6, unix.AF_INET6
		return
	}

	err = ErrUnsupportedProtocol
	return
}
