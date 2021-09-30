package monitor

import "testing"

func TestMetric_Get(t *testing.T) {
	monitor := NewMonitor("boot", "qps", "connection_total")
	monitor.Add("qps", 190)
	monitor.AddInt64("qps", -100)
	monitor.Set("connection_total", 30)

	qps, _ := monitor.Get("qps")
	connectionTotal, _ := monitor.Get("connection_total")
	t.Log(qps, connectionTotal)
}
