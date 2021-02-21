package epoll

type Server interface {
	OnInitComplete()
	OnShutdown()
	OnConnect(conn *Connection)
	OnReceive(conn *Connection, buffer []byte)
	OnClosed(conn *Connection, err error)
}
