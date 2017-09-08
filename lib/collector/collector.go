package collector

import (
	"hash/crc32"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/limiter"
	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/utils"

	pb "github.com/uol/mycenae/lib/proto"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func New(
	log *zap.Logger,
	sts *tsstats.StatsTS,
	cluster *cluster.Cluster,
	meta *meta.Meta,
	cass *depot.Cassandra,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	set *structs.Settings,
	wLimiter *limiter.RateLimit,
) (*Collector, error) {

	gblog = log
	stats = sts

	metaValidationTimeout, err := time.ParseDuration(set.MetaValidationTimeout)
	if err != nil {
		return nil, err
	}

	collect := &Collector{
		boltc:   bc,
		cluster: cluster,
		meta:    meta,
		persist: persistence{
			esearch: es,
			cass:    cass,
		},
		validKey:              regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		validKSID:             regexp.MustCompile(`^[0-9a-z_]+$`),
		settings:              set,
		concPoints:            make(chan struct{}, set.MaxConcurrentPoints),
		wLimiter:              wLimiter,
		metas:                 make(map[string][]*pb.Meta),
		limiter:               ksLimiter{limite: make(map[string]*limiter.RateLimit)},
		metaValidationTimeout: metaValidationTimeout,
	}

	return collect, nil
}

type Collector struct {
	boltc     *bcache.Bcache
	cluster   *cluster.Cluster
	meta      *meta.Meta
	persist   persistence
	validKey  *regexp.Regexp
	validKSID *regexp.Regexp
	settings  *structs.Settings

	concPoints chan struct{}

	receivedSinceLastProbe int64
	errorsSinceLastProbe   int64
	saving                 int64
	shutdown               bool
	wLimiter               *limiter.RateLimit
	limiter                ksLimiter
	metas                  map[string][]*pb.Meta
	mtxMetas               sync.RWMutex
	metaValidationTimeout  time.Duration
}

type ksLimiter struct {
	limite map[string]*limiter.RateLimit
	mtx    sync.RWMutex
}

func (collect *Collector) CheckUDPbind() bool {

	ctxt := gblog.With(
		zap.String("struct", "CollectorV2"),
		zap.String("func", "CheckUDPbind"),
	)

	port := ":" + collect.settings.UDPserverV2.Port

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		ctxt.Error("addr:", zap.Error(err))
	}

	_, err = net.ListenUDP("udp", addr)
	if err != nil {
		ctxt.Debug("", zap.Error(err))
		return true
	}

	return false
}

func (collect *Collector) ReceivedErrorRatio() float64 {

	ctxt := gblog.With(
		zap.String("struct", "CollectorV2"),
		zap.String("func", "ReceivedErrorRatio"),
	)

	y := atomic.LoadInt64(&collect.receivedSinceLastProbe)
	var ratio float64
	if y != 0 {
		ratio = float64(atomic.LoadInt64(&collect.errorsSinceLastProbe) / y)
	}

	ctxt.Debug("", zap.Float64("ratio", ratio))

	atomic.StoreInt64(&collect.receivedSinceLastProbe, 0)
	atomic.StoreInt64(&collect.errorsSinceLastProbe, 0)

	return ratio
}

func (collect *Collector) Stop() {
	collect.shutdown = true
	for {
		if atomic.LoadInt64(&collect.saving) <= 0 {
			return
		}
	}
}

func (collect *Collector) HandlePoint(points gorilla.TSDBpoints) (RestErrors, gobol.Error) {

	start := time.Now()

	returnPoints := RestErrors{}
	var wg sync.WaitGroup

	pts := make(map[string][]*pb.Point, len(points))
	keyspaces := make(map[string]interface{})
	metas := make(map[string][]*pb.Meta)

	var mtx sync.Mutex

	wg.Add(len(points))
	for _, rcvMsg := range points {

		go func(rcvMsg gorilla.TSDBpoint) {
			defer wg.Done()

			ks := "invalid"
			if collect.isKSIDValid(rcvMsg.Tags["ksid"]) {
				ks = rcvMsg.Tags["ksid"]
			}

			atomic.AddInt64(&collect.receivedSinceLastProbe, 1)
			statsPoints(ks, "number")

			packet := &pb.Point{}
			m := &pb.Meta{}

			gerr := collect.makePoint(packet, m, &rcvMsg)
			if gerr != nil {
				mtx.Lock()
				collect.HandleGerr(ks, &returnPoints, rcvMsg, gerr)
				mtx.Unlock()
				return
			}

			nodePoint, gerr := collect.cluster.Classifier([]byte(packet.GetTsid()))
			if gerr != nil {
				mtx.Lock()
				collect.HandleGerr(ks, &returnPoints, rcvMsg, gerr)
				mtx.Unlock()
				return
			}

			var np []byte
			if len(nodePoint) > 1 {
				n0 := nodePoint[0]
				n1 := nodePoint[1]
				np = make([]byte, len(n0)+len(n1)+1)
				copy(np, n0)
				copy(np[len(n0):], "|")
				copy(np[len(n0)+1:], n1)
			}

			nodeMeta, gerr := collect.cluster.MetaClassifier([]byte(m.GetKsid()))
			if gerr != nil {
				mtx.Lock()
				collect.HandleGerr(ks, &returnPoints, rcvMsg, gerr)
				mtx.Unlock()
				return
			}

			mtx.Lock()
			keyspaces[ks] = nil
			pts[string(np)] = append(pts[string(np)], packet)
			if !collect.boltc.Get(utils.KSTS(m.GetKsid(), m.GetTsid())) {
				metas[nodeMeta] = append(metas[nodeMeta], m)
			}
			mtx.Unlock()

		}(rcvMsg)
	}

	wg.Wait()

	for ks := range keyspaces {
		collect.limiter.mtx.RLock()
		l, ok := collect.limiter.limite[ks]
		collect.limiter.mtx.RUnlock()
		if !ok {
			li, err := limiter.New(
				collect.settings.MaxKeyspaceWriteRequests,
				collect.settings.BurstKeyspaceWriteRequests,
				gblog,
			)
			if err != nil {
				gblog.Error(
					err.Error(),
					zap.String("struct", "CollectorV2"),
					zap.String("func", "HandlePoint"),
					zap.Error(err),
				)
				return RestErrors{}, errISE("handlePoint", "unable to create a new limiter", err)
			}
			collect.limiter.mtx.Lock()
			collect.limiter.limite[ks] = li
			collect.limiter.mtx.Unlock()
			l = li
		}
		if gerr := l.Reserve(); gerr != nil {
			return RestErrors{}, gerr
		}

	}

	for n, points := range pts {
		//gblog.Debug("saving map", zap.String("node", n), zap.Any("points", points))
		wg.Add(1)
		go func(n string, points []*pb.Point) {
			defer wg.Done()
			nodes := strings.Split(n, "|")
			collect.cluster.Write(nodes, points)
		}(n, points)
	}

	go func() {
		timeout := time.After(collect.metaValidationTimeout)
		metaCh := make(chan struct{}, 1)
		for n, m := range metas {
			metaCh <- struct{}{}
			select {
			case <-timeout:
				return
			case <-metaCh:
				collect.metaHandler(n, m)
			}
		}
	}()

	wg.Wait()
	for ks := range keyspaces {
		statsProcTime(ks, time.Since(start))
	}

	return returnPoints, nil
}

func (collect *Collector) metaHandler(nodeID string, metas []*pb.Meta) {

	if nodeID == collect.cluster.SelfID() {
		go func() {
			gblog.Debug(
				"processing meta in local node",
				zap.String("struct", "CollectorV2"),
				zap.String("func", "metaHandler"),
				zap.Int("count", len(metas)),
			)
			for _, m := range metas {
				collect.meta.Handle(m)
			}
		}()
		return
	}

	for _, m := range metas {
		ksts := utils.KSTS(m.GetKsid(), m.GetTsid())
		if !collect.boltc.Get(ksts) {

			gblog.Debug(
				"processing meta using gRPC",
				zap.String("struct", "CollectorV2"),
				zap.String("func", "metaHandler"),
				zap.String("node", nodeID),
				zap.Int("count", len(metas)),
			)

			ch, err := collect.cluster.Meta(nodeID, metas)
			if err != nil {
				gblog.Error(
					err.Error(),
					zap.String("struct", "CollectorV2"),
					zap.String("func", "metaHandler"),
					zap.Error(err),
				)
				break
			}
			for mf := range ch {
				if mf.GetOk() {
					if gerr := collect.boltc.Set(mf.GetKsts()); gerr != nil {
						gblog.Error(
							gerr.Error(),
							zap.String("struct", "CollectorV2"),
							zap.String("func", "HandlePoint"),
							zap.Error(gerr),
						)
						continue
					}
				}
			}
		}
	}

}

func (collect *Collector) HandleGerr(ks string, returnPoints *RestErrors, rcvMsg gorilla.TSDBpoint, gerr gobol.Error) {

	atomic.AddInt64(&collect.errorsSinceLastProbe, 1)

	gblog.Error("makePacket", zap.Error(gerr))
	reu := RestErrorUser{
		Datapoint: rcvMsg,
		Error:     gerr.Message(),
	}

	returnPoints.Errors = append(returnPoints.Errors, reu)

	statsPointsError(ks, "number")

}

func (collect *Collector) HandleTxtPacket(rcvMsg gorilla.TSDBpoint) gobol.Error {

	start := time.Now()

	atomic.AddInt64(&collect.receivedSinceLastProbe, 1)

	packet := gorilla.Point{}

	gerr := collect.makePacket(&packet, rcvMsg, false)
	if gerr != nil {
		gblog.Error("makePacket", zap.Error(gerr))
		return gerr
	}

	gerr = collect.saveText(packet)
	if gerr != nil {
		atomic.AddInt64(&collect.errorsSinceLastProbe, 1)
		gblog.Error("save", zap.Error(gerr))
		return gerr
	}

	pkt := &pb.Meta{
		Ksid:   packet.KsID,
		Tsid:   packet.ID,
		Metric: packet.Message.Metric,
	}
	for k, v := range packet.Message.Tags {
		pkt.Tags = append(pkt.Tags, &pb.Tag{Key: k, Value: v})
	}

	go collect.meta.SaveTxtMeta(pkt)

	statsProcTime(packet.KsID, time.Since(start))
	return nil
}

func GenerateID(rcvMsg *gorilla.TSDBpoint) string {

	h := crc32.NewIEEE()

	if rcvMsg.Metric != "" {
		h.Write([]byte(rcvMsg.Metric))
	}

	mk := []string{}

	for k := range rcvMsg.Tags {
		if k != "ksid" && k != "ttl" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {

		h.Write([]byte(k))
		h.Write([]byte(rcvMsg.Tags[k]))

	}

	return strconv.FormatUint(uint64(h.Sum32()), 10)
}
