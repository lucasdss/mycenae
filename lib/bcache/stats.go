package bcache

import (
	"time"

	"github.com/uol/mycenae/lib/tsstats"
)

func statsError(stats *tsstats.StatsTS, oper string, buck []byte) {
	statsIncrement(
		stats, "bolt.query.error",
		map[string]string{"bucket": string(buck), "operation": oper},
	)
}

func statsSuccess(stats *tsstats.StatsTS, oper string, buck []byte, d time.Duration) {
	statsIncrement(stats, "bolt.query", map[string]string{"bucket": string(buck), "operation": oper})
	statsValueAdd(
		stats, "bolt.query.duration",
		map[string]string{"bucket": string(buck), "operation": oper},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsNotFound(stats *tsstats.StatsTS, buck []byte) {
	statsIncrement(
		stats, "bolt.query.not_found",
		map[string]string{"bucket": string(buck)},
	)
}

func statsIncrement(stats *tsstats.StatsTS, metric string, tags map[string]string) {
	stats.Increment("bcache/persistence", metric, tags)
}

func statsValueAdd(stats *tsstats.StatsTS, metric string, tags map[string]string, v float64) {
	stats.ValueAdd("bcache/persistence", metric, tags, v)
}
