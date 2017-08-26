package index

import (
	"time"

	"github.com/uol/mycenae/lib/tsstats"
)

func statsProcTime(stats *tsstats.StatsTS, ks string, d time.Duration, pts int) {
	statsValueAdd(
		stats,
		"points.processes_time",
		map[string]string{"keyspace": ks},
		(float64(d.Nanoseconds())/float64(time.Millisecond))/float64(pts),
	)
}

func statsLostMeta(stats *tsstats.StatsTS) {
	statsIncrement(
		stats,
		"meta.lost",
		map[string]string{},
	)
}

func statsIndexError(stats *tsstats.StatsTS, i, t, m string) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	statsIncrement(stats, "elastic.request.error", tags)
}

func statsIndex(stats *tsstats.StatsTS, i, t, m string, d time.Duration) {
	tags := map[string]string{"method": m}
	if i != "" {
		tags["index"] = i
	}
	if t != "" {
		tags["type"] = t
	}
	statsIncrement(stats, "elastic.request", tags)
	statsValueAdd(
		stats,
		"elastic.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsBulkPoints(stats *tsstats.StatsTS) {
	statsIncrement(stats, "elastic.bulk.points", map[string]string{})
}

func statsIncrement(stats *tsstats.StatsTS, metric string, tags map[string]string) {
	if stats != nil {
		stats.Increment("meta", metric, tags)
	}
}

func statsValueAdd(stats *tsstats.StatsTS, metric string, tags map[string]string, v float64) {
	if stats != nil {
		stats.ValueAdd("meta", metric, tags, v)
	}
}
