package meta

import (
	"time"
)

func statsProcTime(ks string, d time.Duration, pts int) {
	statsValueAdd(
		"points.processes_time",
		map[string]string{"keyspace": ks},
		(float64(d.Nanoseconds())/float64(time.Millisecond))/float64(pts),
	)
}

func statsLostMeta() {
	statsIncrement(
		"meta.lost",
		map[string]string{},
	)
}

func statsIndexError(i, t, m string) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	statsIncrement("elastic.request.error", tags)
}

func statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	statsIncrement("elastic.request", tags)
	statsValueAdd(
		"elastic.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsBulkPoints() {
	statsIncrement("elastic.bulk.points", map[string]string{})
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("meta", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("meta", metric, tags, v)
}
