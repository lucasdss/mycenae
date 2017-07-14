package tsstats

import (
	"github.com/uol/gobol/snitch"
	"go.uber.org/zap"
	"gopkg.in/robfig/cron.v2"
)

func New(gbl *zap.Logger, gbs *snitch.Stats, intvl string) (*StatsTS, error) {
	if _, err := cron.Parse(intvl); err != nil {
		return nil, err
	}
	return &StatsTS{
		log:      gbl,
		stats:    gbs,
		interval: intvl,
	}, nil
}

type StatsTS struct {
	stats    *snitch.Stats
	log      *zap.Logger
	interval string
}

func (sts *StatsTS) Increment(callerID string, metric string, tags map[string]string) {
	err := sts.stats.Increment(metric, tags, sts.interval, false, true)
	if err != nil {
		sts.log.Error(
			"",
			zap.String("package", callerID),
			zap.String("func", "statsIncrement"),
			zap.String("metric", metric),
			zap.Error(err),
		)
	}
}

func (sts *StatsTS) ValueAdd(callerID string, metric string, tags map[string]string, v float64) {
	err := sts.stats.ValueAdd(metric, tags, "avg", sts.interval, false, false, v)
	if err != nil {
		sts.log.Error(
			"",
			zap.String("package", callerID),
			zap.String("func", "statsValueAdd"),
			zap.String("metric", metric),
			zap.Error(err),
		)
	}
}
