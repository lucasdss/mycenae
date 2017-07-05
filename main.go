package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/uol/gobol/loader"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/saw"
	"github.com/uol/gobol/snitch"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/limiter"
	"github.com/uol/mycenae/lib/plot"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/rest"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/udp"
	"github.com/uol/mycenae/lib/udpError"
)

func main() {

	log.Println("Starting Mycenae")

	//Parse of command line arguments.
	var confPath string

	flag.StringVar(&confPath, "config", "config.toml", "path to configuration file")
	flag.Parse()

	//Load conf file.
	settings := new(structs.Settings)

	err := loader.ConfToml(confPath, &settings)
	if err != nil {
		log.Fatal("ERROR - Loading Config file: ", err)
	} else {
		log.Println("Config file loaded.")
	}

	tsLogger, err := saw.New(settings.Logs.LogLevel, settings.Logs.Environment)
	if err != nil {
		log.Fatal("ERROR - Starting logger: ", err)
	}

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6666", nil))
	}()

	sts, err := snitch.New(tsLogger, settings.Stats)
	if err != nil {
		tsLogger.Fatal("ERROR - Starting stats: ", zap.Error(err))
	}

	tssts, err := tsstats.New(tsLogger, sts, settings.Stats.Interval)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}

	rcs, err := parseConsistencies(settings.ReadConsistency)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}

	wcs, err := parseConsistencies(settings.WriteConsisteny)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}

	d, err := depot.NewCassandra(
		settings.Cassandra,
		rcs,
		wcs,
		tsLogger,
		tssts,
	)
	if err != nil {
		tsLogger.Fatal("ERROR - Connecting to cassandra: ", zap.Error(err))
	}
	defer d.Close()

	es, err := rubber.New(tsLogger, settings.ElasticSearch.Cluster)
	if err != nil {
		tsLogger.Fatal("ERROR - Connecting to elasticsearch: ", zap.Error(err))
	}

	ks := keyspace.New(
		tssts,
		d.Session,
		es,
		settings.Cassandra.Username,
		settings.Cassandra.Keyspace,
		settings.CompactionStrategy,
		settings.TTL.Max,
	)

	bc, err := bcache.New(tssts, ks, settings.BoltPath)
	if err != nil {
		tsLogger.Fatal("", zap.Error(err))
	}

	wal, err := gorilla.NewWAL(settings.WALPath)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}
	wal.Start()

	strg := gorilla.New(tsLogger, tssts, d, wal)
	strg.Load()

	cluster, err := cluster.New(tsLogger, strg, settings.Cluster)
	if err != nil {
		tsLogger.Fatal("", zap.Error(err))
	}

	limiter, err := limiter.New(settings.MaxRateLimit, settings.Burst, tsLogger)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}

	coll, err := collector.New(tsLogger, tssts, cluster, d, es, bc, settings, limiter)
	if err != nil {
		tsLogger.Fatal(err.Error())
	}

	uV2server := udp.New(tsLogger, settings.UDPserverV2, coll)
	uV2server.Start()

	collectorV1 := collector.UDPv1{}

	uV1server := udp.New(tsLogger, settings.UDPserver, collectorV1)
	uV1server.Start()

	p, err := plot.New(
		tsLogger,
		tssts,
		cluster,
		es,
		bc,
		settings.ElasticSearch.Index,
		settings.MaxTimeseries,
		settings.MaxConcurrentTimeseries,
		settings.MaxConcurrentReads,
		settings.LogQueryTSthreshold,
	)
	if err != nil {
		tsLogger.Fatal("", zap.Error(err))
	}

	uError := udpError.New(
		tsLogger,
		tssts,
		d.Session,
		bc,
		es,
		settings.ElasticSearch.Index,
		rcs,
	)

	tsRest := rest.New(
		tsLogger,
		sts,
		p,
		uError,
		ks,
		bc,
		coll,
		settings.HTTPserver,
		settings.Probe.Threshold,
	)
	tsRest.Start()

	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	tsLogger.Info("Mycenae started successfully")

	go func() {
		for pts := range wal.Load() {
			if len(pts) > 0 {
				tsLogger.Debug(
					"loading points from commitlog",
					zap.Int("count", len(pts)),
					zap.String("func", "main"),
					zap.String("package", "main"),
				)
			}

			for _, p := range pts {
				err := cluster.WAL(&pb.TSPoint{Tsid: p.TSID, Ksid: p.KSID, Date: p.T, Value: p.V})
				if err != nil {
					tsLogger.Error(
						"failure loading point from write-ahead-log",
						zap.String("package", "main"),
						zap.String("func", "main"),
						zap.Error(err),
					)
				}
			}
		}

		tsLogger.Debug(
			"finished loading points from commitlog",
			zap.String("func", "main"),
			zap.String("package", "main"),
		)

	}()

	for {
		sig := <-signalChannel
		switch sig {
		case os.Interrupt, syscall.SIGTERM:
			stop(tsLogger, tsRest, coll, strg)
			return
		case syscall.SIGHUP:
			//THIS IS A HACK DO NOT EXTEND IT. THE FEATURE IS NICE BUT NEEDS TO BE DONE CORRECTLY!!!!!
			settings := new(structs.Settings)
			var err error

			if strings.HasSuffix(confPath, ".json") {
				err = loader.ConfJson(confPath, &settings)
			} else if strings.HasSuffix(confPath, ".toml") {
				err = loader.ConfToml(confPath, &settings)
			}
			if err != nil {
				tsLogger.Error("ERROR - Loading Config file: ", zap.Error(err))
				continue
			} else {
				tsLogger.Info("Config file loaded.")
			}

			rcs, err := parseConsistencies(settings.ReadConsistency)
			if err != nil {
				tsLogger.Error(err.Error())
				continue
			}

			wcs, err := parseConsistencies(settings.WriteConsisteny)
			if err != nil {
				tsLogger.Error(err.Error())
				continue
			}

			d.SetWriteConsistencies(wcs)

			d.SetReadConsistencies(rcs)

			tsLogger.Info("New consistency set")

		}
	}
}

func parseConsistencies(names []string) ([]gocql.Consistency, error) {
	if len(names) == 0 {
		return nil, errors.New("consistency array cannot be empty")
	}

	if len(names) > 3 {
		return nil, errors.New("consistency array too big")
	}

	tmp := make([]gocql.Consistency, len(names))
	for i, cons := range names {
		cons = strings.ToLower(cons)

		switch cons {
		case "one":
			tmp[i] = gocql.One
		case "quorum":
			tmp[i] = gocql.Quorum
		case "all":
			tmp[i] = gocql.All
		default:
			return nil, fmt.Errorf("error: unknown consistency: %s", cons)
		}
	}
	return tmp, nil
}

func stop(
	logger *zap.Logger,
	rest *rest.REST,
	collector *collector.Collector,
	strg *gorilla.Storage,
) {

	logger.Info("Stopping REST")
	rest.Stop()

	logger.Info("Stopping UDPv2")
	collector.Stop()

	logger.Info("Stopping storage")
	strg.Stop()

}
