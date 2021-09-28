package monitor

type Monitor struct {
	appName    string
	metricList map[string]*Metric
}

func (m *Monitor) Add(name string, val uint64) {
	if _, exists := m.metricList[name]; exists {
		m.metricList[name].Add(val)
	}
}

func (m *Monitor) Set(name string, val uint64) {
	if _, exists := m.metricList[name]; exists {
		m.metricList[name].Set(val)
	}
}

func (m *Monitor) GetMetric(name string) (metric *Metric, exists bool) {
	metric, exists = m.metricList[name]
	return
}

func (m *Monitor) Get(name string) (val uint64, exists bool) {
	if _, exists = m.metricList[name]; !exists {
		return
	}
	val = m.metricList[name].Get()
	return
}
