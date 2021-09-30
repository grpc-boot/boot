package monitor

type Monitor struct {
	appName    string
	metricList map[string]*Metric
}

func NewMonitor(appName string, nameList ...string) (m *Monitor) {
	m = &Monitor{
		appName:    appName,
		metricList: make(map[string]*Metric, len(nameList)),
	}

	for _, name := range nameList {
		m.metricList[name] = &Metric{}
	}

	return
}

func (m *Monitor) AddInt64(name string, val int64) (newValue uint64, exists bool) {
	_, exists = m.metricList[name]
	if exists {
		return m.metricList[name].AddInt64(val), exists
	}

	return 0, exists
}

func (m *Monitor) Add(name string, val uint64) (newValue uint64, exists bool) {
	_, exists = m.metricList[name]
	if exists {
		return m.metricList[name].Add(val), exists
	}

	return 0, exists
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
