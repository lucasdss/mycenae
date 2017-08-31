package cluster

import (
	"time"
)

func statsProcTime(method string, d time.Duration) {
	statsValueAdd(
		"request.duration",
		map[string]string{"path": method, "protocol": "gRPC"},
		float64(d) / float64(time.Millisecond),
	)
}

func statsProcCount(method, status string) {
	statsIncrement(
		"request.count",
		map[string]string{"path": method, "status": status, "protocol": "gRPC"},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("cluster", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("cluster", metric, tags, v)
}
