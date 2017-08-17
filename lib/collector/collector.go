package collector

import (
	"hash/crc32"
	"net"
	"regexp"
	"sort"
	"strconv"
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

	collect := &Collector{
		boltc:   bc,
		cluster: cluster,
		meta:    meta,
		persist: persistence{
			esearch: es,
			cass:    cass,
		},
		validKey:   regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		validKSID:  regexp.MustCompile(`^[0-9a-z_]+$`),
		settings:   set,
		concPoints: make(chan struct{}, set.MaxConcurrentPoints),
		wLimiter:   wLimiter,
		metas:      make(map[string][]*pb.Meta),
		limiter:    ksLimiter{limite: make(map[string]*limiter.RateLimit)},
	}
	collect.metaHandler()

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
		if collect.saving <= 0 {
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

			nodeMeta, gerr := collect.cluster.MetaClassifier([]byte(m.GetKsid()))
			if gerr != nil {
				mtx.Lock()
				collect.HandleGerr(ks, &returnPoints, rcvMsg, gerr)
				mtx.Unlock()
				return
			}

			collect.metaQueue(nodeMeta, m)

			mtx.Lock()
			keyspaces[ks] = nil
			for _, np := range nodePoint {
				pts[np] = append(pts[np], packet)
			}
			mtx.Unlock()

			statsProcTime(m.GetKsid(), time.Since(start), len(points))

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
		collect.cluster.Write(n, points)
	}

	return returnPoints, nil

}

func (collect *Collector) metaHandler() {
	go func() {

		ticker := time.NewTicker(time.Second)
		for {

			var dequeue []string
			var mtx sync.Mutex
			select {
			case <-ticker.C:

				collect.mtxMetas.Lock()
				for nodeID, metas := range collect.metas {
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
								mtx.Lock()
								dequeue = append(dequeue, nodeID)
								mtx.Unlock()
							}
						}()
						continue
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
					mtx.Lock()
					dequeue = append(dequeue, nodeID)
					mtx.Unlock()

				}
				collect.mtxMetas.Unlock()
			}

			mtx.Lock()
			for _, nodeID := range dequeue {
				collect.metaDequeue(nodeID)
			}
			mtx.Unlock()

		}
	}()
}

func (collect *Collector) metaQueue(nodeID string, m *pb.Meta) {

	if !collect.boltc.Get(utils.KSTS(m.GetKsid(), m.GetTsid())) {

		collect.mtxMetas.Lock()
		defer collect.mtxMetas.Unlock()

		if len(collect.metas[nodeID]) >= 100000 {
			gblog.Debug(
				"dropping meta, buffer too big",
				zap.String("struct", "CollectorV2"),
				zap.String("func", "metaQueue"),
				zap.Int("size", len(collect.metas[nodeID])),
			)
			return
		}
		collect.metas[nodeID] = append(collect.metas[nodeID], m)
	}

}

func (collect *Collector) metaDequeue(nodeID string) {
	collect.mtxMetas.Lock()
	defer collect.mtxMetas.Unlock()

	delete(collect.metas, nodeID)
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

	statsProcTime(packet.KsID, time.Since(start), 1)
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
