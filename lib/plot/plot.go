package plot

import (
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/tsstats"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func New(
	gbl *zap.Logger,
	sts *tsstats.StatsTS,
	cluster *cluster.Cluster,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	esIndex string,
	maxTimeseries int,
	maxConcurrentTimeseries int,
	maxConcurrentReads int,
	logQueryTSthreshold int,
) (*Plot, gobol.Error) {

	gblog = gbl
	stats = sts

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if maxConcurrentReads < 1 {
		return nil, errInit("MaxConcurrentReads needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	if maxConcurrentTimeseries > maxConcurrentReads {
		return nil, errInit("maxConcurrentTimeseries cannot be bigger than maxConcurrentReads")
	}

	return &Plot{
		esIndex:           esIndex,
		MaxTimeseries:     maxTimeseries,
		LogQueryThreshold: logQueryTSthreshold,
		boltc:             bc,
		persist:           persistence{cluster: cluster, esTs: es},
		concTimeseries:    make(chan struct{}, maxConcurrentTimeseries),
		concReads:         make(chan struct{}, maxConcurrentReads),
	}, nil
}

type Plot struct {
	esIndex           string
	MaxTimeseries     int
	LogQueryThreshold int
	boltc             *bcache.Bcache
	persist           persistence
	concTimeseries    chan struct{}
	concReads         chan struct{}
}
