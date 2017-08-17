package structs

import (
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/wal"
)

type SettingsHTTP struct {
	Path string
	Port string
	Bind string
}

type SettingsUDP struct {
	Port       string
	ReadBuffer int
}

type Settings struct {
	ReadConsistency            []string
	WriteConsisteny            []string
	BoltPath                   string
	WAL                        *wal.Settings
	MaxTimeseries              int
	MaxConcurrentTimeseries    int
	MaxKeyspaceWriteRequests   int
	BurstKeyspaceWriteRequests int
	MaxConcurrentReads         int
	MaxConcurrentPoints        int
	LogQueryTSthreshold        int
	MaxRateLimit               int
	Burst                      int
	CompactionStrategy         string
	Meta                       *meta.Settings
	HTTPserver                 SettingsHTTP
	UDPserver                  SettingsUDP
	UDPserverV2                SettingsUDP
	Depot                      depot.Settings
	Cluster                    cluster.Config
	TTL                        struct {
		Max int
	}
	Logs struct {
		Environment string
		LogLevel    string
	}
	Stats     snitch.Settings
	StatsFile struct {
		Path string
	}
	ElasticSearch struct {
		Cluster rubber.Settings
		Index   string
	}
	Probe struct {
		Threshold float64
	}
}
