package bcache

import (
	"time"
)

func statsError(oper string, buck []byte) {
	statsIncrement(
		"bolt.query.error",
		map[string]string{"bucket": string(buck), "operation": oper},
	)
}

func statsSuccess(oper string, buck []byte, d time.Duration) {
	statsIncrement("bolt.query", map[string]string{"bucket": string(buck), "operation": oper})
	statsValueAdd(
		"bolt.query.duration",
		map[string]string{"bucket": string(buck), "operation": oper},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsNotFound(buck []byte) {
	statsIncrement(
		"bolt.query.not_found",
		map[string]string{"bucket": string(buck)},
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("bcache/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("bcache/persistence", metric, tags, v)
}
