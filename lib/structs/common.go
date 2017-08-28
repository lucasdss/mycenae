package structs

import (
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/snitch"
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

// MetaSettings defines the settings for elasticsearch backend
type MetaSettings struct {
	MetaSaveInterval    string
	MaxConcurrentBulks  int
	MaxConcurrentPoints int
	MaxMetaBulkSize     int
	MetaBufferSize      int
	MetaHeadInterval    string
}

type WALSettings struct {
	PathWAL        string
	CheckPointPath string

	SyncInterval       string
	CleanupInterval    string
	CheckPointInterval string
	MaxBufferSize      int
	MaxConcWrite       int
}

type ConsulConfig struct {
	//Consul agent adrress without the scheme
	Address string
	//Consul agent port
	Port int
	//Location of consul agent cert file
	Cert string
	//Location of consul agent key file
	Key string
	//Location of consul agent CA file
	CA string
	//Name of the service to be probed on consul
	Service string
	//Tag of the service
	Tag string
	// Token of the service
	Token string
	// Protocol of the service
	Protocol string
}

type ClusterConfig struct {
	Consul ConsulConfig
	//gRPC port
	Port int
	//Ticker interval to check cluster changes
	CheckInterval string
	//Time, in seconds, to wait before applying cluster changes to consistency hashing
	ApplyWait int64

	GrpcTimeout         string
	GrpcMaxServerConn   int64
	GrpcBurstServerConn int
	MaxListenerConn     int
}

type DepotSettings struct {
	Cassandra     cassandra.Settings
	MaxConcurrent int
}

type Settings struct {
	ReadConsistency            []string
	WriteConsisteny            []string
	BoltPath                   string
	WAL                        *WALSettings
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
	Meta                       *MetaSettings
	HTTPserver                 SettingsHTTP
	UDPserver                  SettingsUDP
	UDPserverV2                SettingsUDP
	Depot                      DepotSettings
	Cluster                    ClusterConfig
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
