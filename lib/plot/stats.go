package plot

import (
	"time"
)

func statsQueryThreshold(ks string) {
	statsIncrement(
		"mycenae.query.threshold",
		map[string]string{"keyspace": ks},
	)
}

func statsQueryLimit(ks string) {
	statsIncrement(
		"mycenae.query.limit",
		map[string]string{"keyspace": ks},
	)
}

func statsSelectQerror(ks, cf string) {
	statsIncrement(
		"cassandra.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsSelectFerror(ks, cf string) {
	statsIncrement(
		"cassandra.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsIndexError(i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	statsIncrement("elastic.request.error", tags)
}

func statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
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

func statsSelect(ks, cf string, d time.Duration) {
	statsIncrement("cassandra.query", map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"})
	statsValueAdd(
		"cassandra.query.duration",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("plot/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("plot/persistence", metric, tags, v)
}
