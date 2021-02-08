package epoll

type Manager struct {
	reactors    []*Epoll
	connections *ConnectionMap
}

func NewManager(max int) *Manager {
	manager := &Manager{
		reactors:    make([]*Epoll, max, max),
		connections: NewConnectionMap(1),
	}

	for index := 0; index < max; index++ {
		manager.reactors[index], _ = NewEpoll()
	}

	return manager
}

func (m *Manager) Add(conn *Connection) {
	conn.reactorId = conn.fd & len(m.reactors)
	m.reactors[conn.reactorId].AddRead(conn.fd)
	m.connections.Add(conn)
}

func (m *Manager) Remove(fd int) {
	m.reactors[fd*len(m.reactors)].Remove(fd)
	m.connections.Del(fd)
}

func (m *Manager) Start() {
	for index, _ := range m.reactors {
		go m.reactors[index].Poll(func(fd int, event uint8) {

		})
	}
}
