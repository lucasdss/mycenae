package cluster

import (
	"time"
)

func statsProcTime(method string, d time.Duration) {
	statsValueAdd(
		"grpc_server.processes_time",
		map[string]string{"method": method},
		float64(d) / float64(time.Millisecond),
	)
}

func statsProcError(method string) {
	statsIncrement(
		"grpc_server.error",
		map[string]string{"method": method},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("cluster", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("cluster", metric, tags, v)
}
