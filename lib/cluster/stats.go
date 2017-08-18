package cluster

import (
	"time"
)

func statsProcTime(method string, d time.Duration) {
	statsValueAdd(
		"request.duration",
		map[string]string{"path": method, "protocol": "grpc"},
		float64(d) / float64(time.Millisecond),
	)
}

func statsProcCount(method, status string) {
	statsIncrement(
		"request.count",
		map[string]string{"method": method, "status": status, "protocol": "grpc"},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("cluster", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("cluster", metric, tags, v)
}
