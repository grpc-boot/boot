package epoll

import (
	"github.com/grpc-boot/boot/container"
)

type Manager struct {
	eventLoops  []EventLoop
	connections *container.Map
}

func NewManager(max int) *Manager {
	manager := &Manager{
		eventLoops:  make([]EventLoop, max, max),
		connections: container.NewMap(),
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

func (m *Manager) TotalConnections() (count uint64) {
	return m.connections.Length()
}
