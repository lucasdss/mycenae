package wal

func statsCountWrite(tags map[string]string, value float64) {
	statsValueAdd(
		"wal.write.count",
		tags,
		value,
	)
}

func statsSegmentSize(tags map[string]string, value float64) {
	statsValueAdd(
		"wal.segment.size",
		tags,
		value,
	)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("wal", metric, tags, v)
}
