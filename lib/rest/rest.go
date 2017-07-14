package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/gobol/snitch"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/config"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/udpError"

	"go.uber.org/zap"
)

func New(
	log *zap.Logger,
	gbs *snitch.Stats,
	p *plot.Plot,
	ue *udpError.UDPerror,
	keyspace *keyspace.Keyspace,
	bc *bcache.Bcache,
	collector *collector.Collector,
	set structs.SettingsHTTP,
	probeThreshold float64,
) *REST {
	return &REST{
		probeThreshold: probeThreshold,
		probeStatus:    http.StatusOK,
		closed:         make(chan struct{}),

		gblog:    log,
		sts:      gbs,
		reader:   p,
		udperr:   ue,
		kspace:   keyspace,
		boltc:    bc,
		writer:   collector,
		settings: set,
	}
}

type REST struct {
	probeThreshold float64
	probeStatus    int
	closed         chan struct{}

	gblog    *zap.Logger
	sts      *snitch.Stats
	reader   *plot.Plot
	udperr   *udpError.UDPerror
	kspace   *keyspace.Keyspace
	boltc    *bcache.Bcache
	writer   *collector.Collector
	settings structs.SettingsHTTP
	server   *http.Server
}

func (trest *REST) Start() {

	go trest.asyncStart()

}

func (trest *REST) asyncStart() {

	rip.SetLogger(trest.gblog)

	pathMatcher := regexp.MustCompile(`^(/[a-zA-Z0-9._-]+)?/$`)

	if !pathMatcher.Match([]byte(trest.settings.Path)) {
		err := errors.New("Invalid path to start rest service")
		trest.gblog.Fatal("ERROR - Starting REST: ", zap.Error(err))
	}

	path := trest.settings.Path

	router := rip.NewCustomRouter()
	//PROBE
	router.GET(path+"probe", trest.check)
	//READ
	router.POST(path+"keyspaces/:keyspace/points", trest.reader.ListPoints)
	//EXPRESSION
	router.GET(path+"expression/check", trest.reader.ExpressionCheckGET)
	router.POST(path+"expression/check", trest.reader.ExpressionCheckPOST)
	router.POST(path+"expression/compile", trest.reader.ExpressionCompile)
	router.GET(path+"expression/parse", trest.reader.ExpressionParseGET)
	router.POST(path+"expression/parse", trest.reader.ExpressionParsePOST)
	router.GET(path+"keyspaces/:keyspace/expression/expand", trest.reader.ExpressionExpandGET)
	router.POST(path+"keyspaces/:keyspace/expression/expand", trest.reader.ExpressionExpandPOST)
	//NUMBER
	router.GET(path+"keyspaces/:keyspace/tags", trest.reader.ListTagsNumber)
	router.GET(path+"keyspaces/:keyspace/metrics", trest.reader.ListMetricsNumber)
	router.POST(path+"keyspaces/:keyspace/meta", trest.reader.ListMetaNumber)
	//TEXT
	router.GET(path+"keyspaces/:keyspace/text/tags", trest.reader.ListTagsText)
	router.GET(path+"keyspaces/:keyspace/text/metrics", trest.reader.ListMetricsText)
	router.POST(path+"keyspaces/:keyspace/text/meta", trest.reader.ListMetaText)
	//UDP ERROR
	router.POST(path+"keyspaces/:keyspace/errortags", trest.udperr.ListErrorTags)
	router.GET(path+"keyspaces/:keyspace/errors/:error", trest.udperr.GetErrorInfo)
	//KEYSPACE
	router.GET(path+"datacenters", trest.kspace.ListDC)
	router.HEAD(path+"keyspaces/:keyspace", trest.kspace.Check)
	router.POST(path+"keyspaces/:keyspace", trest.kspace.Create)
	router.PUT(path+"keyspaces/:keyspace", trest.kspace.Update)
	router.GET(path+"keyspaces", trest.kspace.GetAll)
	//WRITE
	router.POST(path+"api/put", trest.writer.Scollector)
	router.PUT(path+"api/put", trest.writer.Scollector)
	router.POST(path+"v2/points", trest.writer.Scollector)
	router.POST(path+"v2/text", trest.writer.Text)
	//OPENTSDB
	router.POST("/keyspaces/:keyspace/api/query", trest.reader.Query)
	router.GET("/keyspaces/:keyspace/api/suggest", trest.reader.Suggest)
	router.GET("/keyspaces/:keyspace/api/search/lookup", trest.reader.Lookup)
	router.GET("/keyspaces/:keyspace/api/aggregators", config.Aggregators)
	router.GET("/keyspaces/:keyspace/api/config/filters", config.Filters)
	//HYBRIDS
	router.POST("/keyspaces/:keyspace/query/expression", trest.reader.ExpressionQueryPOST)
	router.GET("/keyspaces/:keyspace/query/expression", trest.reader.ExpressionQueryGET)

	trest.server = &http.Server{
		Addr: fmt.Sprintf("%s:%s", trest.settings.Bind, trest.settings.Port),
		Handler: rip.NewLogMiddleware(
			"mycenae",
			"mycenae",
			trest.gblog,
			trest.sts,
			rip.NewGzipMiddleware(rip.BestSpeed, router),
		),
	}

	err := trest.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		trest.gblog.Error("Error ListenAndServe", zap.Error(err))
	}

	trest.closed <- struct{}{}
}

func (trest *REST) check(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	ratio := trest.writer.ReceivedErrorRatio()

	UDPup := trest.writer.CheckUDPbind()

	if UDPup && ratio < trest.probeThreshold {
		w.WriteHeader(trest.probeStatus)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (trest *REST) Stop() {

	trest.probeStatus = http.StatusServiceUnavailable

	if err := trest.server.Shutdown(context.Background()); err != nil {
		trest.gblog.Error("Shutdown", zap.Error(err))
	}

	<-trest.closed
}
