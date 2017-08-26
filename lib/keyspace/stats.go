package keyspace

import (
	"time"

	"github.com/uol/mycenae/lib/tsstats"
)

func statsQueryError(stats *tsstats.StatsTS, ks, cf, oper string) {
	tags := map[string]string{"keyspace": ks, "operation": oper}
	if cf != "" {
		tags["column_family"] = cf
	}
	go statsIncrement(stats, "cassandra.query.error", tags)
}

func statsQuery(stats *tsstats.StatsTS, ks, cf, oper string, d time.Duration) {
	tags := map[string]string{"keyspace": ks, "operation": oper}
	if cf != "" {
		tags["column_family"] = cf
	}
	go statsIncrement(stats, "cassandra.query", tags)
	go statsValueAdd(
		stats, "cassandra.query.duration", tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIndexError(stats *tsstats.StatsTS, i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement(stats, "elastic.request.error", tags)
}

func statsIndex(stats *tsstats.StatsTS, i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement(stats, "elastic.request", tags)
	go statsValueAdd(
		stats, "elastic.request.duration", tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIncrement(stats *tsstats.StatsTS, metric string, tags map[string]string) {
	stats.Increment("keyspace/persistence", metric, tags)
}

func statsValueAdd(stats *tsstats.StatsTS, metric string, tags map[string]string, v float64) {
	stats.ValueAdd("keyspace/persistence", metric, tags, v)
}
