package meta

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/bcache"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

func makeTestElastic() (*elasticMeta, error) {
	logger := zap.NewNop()

	gbstats, err := snitch.New(logger, snitch.Settings{
		Address:          "localhost",
		Port:             "8787",
		Protocol:         "http",
		HTTPTimeout:      time.Minute.String(),
		HTTPPostInterval: (3 * time.Second).String(),
		Tags: map[string]string{
			"testing": "true",
		},
		KSID:     "stats",
		Interval: (3 * time.Second).String(),
		Runtime:  true,
	})
	if err != nil {
		return nil, err
	}

	stats, err := tsstats.New(logger, gbstats, "@every 1m")
	if err != nil {
		return nil, err
	}

	elastic, err := rubber.New(logger, rubber.Settings{
		Seed:    "172.17.0.6:9200",
		Type:    rubber.ConfigWeightedBackend,
		Timeout: time.Minute,
	})
	if err != nil {
		return nil, err
	}

	cache, err := bcache.New(stats, "/tmp/test-bcache")
	if err != nil {
		return nil, err
	}

	meta, err := createElasticMeta(logger, stats, elastic, cache, &structs.MetaSettings{
		MetaSaveInterval:    time.Second.String(),
		MaxConcurrentBulks:  1,
		MaxConcurrentPoints: 1024,
		MaxMetaBulkSize:     1024 * 1024,
		MetaBufferSize:      1024 * 1024,
		MetaHeadInterval:    time.Second.String(),
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func makeTestLocal() (*localMeta, error) {
	config := zap.NewDevelopmentConfig()
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	return createLocal(logger), nil
}

func genericBackendTesting(t *testing.T, backend Backend) {
	const (
		ksid = "stats"
		tsid = "1010100101010"
	)

	meta := Create(backend)
	if !assert.NoError(t, meta.CreateIndex(ksid)) {
		return
	}
	assert.NotNil(t, meta)

	meta.Handle(&pb.Meta{
		Ksid: ksid,
		Tsid: tsid,

		Metric: "testing.metric",
		Tags: []*pb.Tag{
			{Key: "host", Value: "nohost"},
		},
	})
	time.Sleep(time.Second * 5)

	found, err := meta.CheckTSID(elasticMetaType, fmt.Sprintf("%s|%d", ksid, 0))
	assert.NoError(t, err)
	assert.False(t, found)

	found, err = meta.CheckTSID(elasticMetaType, fmt.Sprintf("%s|%s", ksid, tsid))
	assert.NoError(t, err)
	assert.True(t, found)
}

func TestLocalBackend(t *testing.T) {
	backend, err := makeTestLocal()
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, backend)
	genericBackendTesting(t, backend)
}

func TestElasticBackend(t *testing.T) {
	backend, err := makeTestElastic()
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, backend)
	genericBackendTesting(t, backend)
}
