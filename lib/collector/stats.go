package collector

import (
	"time"
)

func statsUDPv1() {
	statsIncrement(
		"points.received",
		map[string]string{"protocol": "udp", "api": "v1"},
	)
}

func statsUDP(ks, vt string) {
	statsIncrement(
		"points.received",
		map[string]string{"protocol": "udp", "api": "v2", "keyspace": ks, "type": vt},
	)
}

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

func statsUDPerror(ks, vt string) {
	statsIncrement(
		"points.received.error",
		map[string]string{"protocol": "udp", "api": "v2", "keyspace": ks, "type": vt},
	)
}

func statsInsertQerror(ks, cf string) {
	statsIncrement(
		"cassandra.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "insert"},
	)
}

func statsInsertFBerror(ks, cf string) {
	statsIncrement(
		"cassandra.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "insert"},
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

func statsInsert(ks, cf string, d time.Duration) {
	statsIncrement("cassandra.query", map[string]string{"keyspace": ks, "column_family": cf, "operation": "insert"})
	statsValueAdd(
		"cassandra.query.duration",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "insert"},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsPoints(ks, vt string) {
	statsIncrement(
		"points.received",
		map[string]string{"protocol": "http", "api": "v2", "keyspace": ks, "type": vt},
	)
}

func statsPointsError(ks, vt string) {
	statsIncrement(
		"points.received.error",
		map[string]string{"protocol": "http", "api": "v2", "keyspace": ks, "type": vt},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("collector", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("collector", metric, tags, v)
}
