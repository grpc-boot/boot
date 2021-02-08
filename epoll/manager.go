package epoll

type Reactor struct {
	eventLoops  []EventLoop
	connections *ConnectionMap
}

func NewManager(max int) *Reactor {
	manager := &Reactor{
		eventLoops:  make([]EventLoop, max, max),
		connections: NewConnectionMap(1),
	}

	for index := 0; index < max; index++ {
		poller, _ := NewEpoll()
		manager.eventLoops[index] = &eventLoop{
			poller:      poller,
			connections: manager.connections,
		}
	}

	return manager
}

func (m *Reactor) Start() {

}
