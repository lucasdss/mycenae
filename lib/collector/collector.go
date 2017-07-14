package collector

import (
	"fmt"
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
	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/limiter"

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
	wLimiter *limiter.RateLimite,
) (*Collector, error) {

	gblog = log
	stats = sts

	collect := &Collector{
		boltc:   bc,
		cluster: cluster,
		meta:    meta,
		persist: persistence{
			cluster: cluster,
			esearch: es,
			cass:    cass,
		},
		validKey:   regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		settings:   set,
		concPoints: make(chan struct{}, set.MaxConcurrentPoints),
		wLimiter:    wLimiter,
	}

	return collect, nil
}

type Collector struct {
	boltc    *bcache.Bcache
	cluster  *cluster.Cluster
	meta     *meta.Meta
	persist  persistence
	validKey *regexp.Regexp
	settings *structs.Settings

	concPoints chan struct{}

	receivedSinceLastProbe int64
	errorsSinceLastProbe   int64
	saving                 int64
	shutdown               bool
	wLimiter               *limiter.RateLimite
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

func (collect *Collector) HandlePoint(points gorilla.TSDBpoints) RestErrors {

	start := time.Now()

	returnPoints := RestErrors{}
	var wg sync.WaitGroup

	pts := make([]*pb.TSPoint, len(points))

	mm := make(map[string]*pb.Meta)
	var mtx sync.Mutex

	wg.Add(len(points))
	for i, rcvMsg := range points {

		atomic.AddInt64(&collect.receivedSinceLastProbe, 1)
		statsPoints(rcvMsg.Tags["ksid"], "number")

		go func(rcvMsg gorilla.TSDBpoint, i int) {
			defer wg.Done()

			packet := &pb.TSPoint{}
			m := &pb.Meta{}

			gerr := collect.makePoint(packet, m, &rcvMsg)
			if gerr != nil {
				atomic.AddInt64(&collect.errorsSinceLastProbe, 1)

				gblog.Error("makePacket", zap.Error(gerr))
				reu := RestErrorUser{
					Datapoint: rcvMsg,
					Error:     gerr.Message(),
				}
				mtx.Lock()
				returnPoints.Errors = append(returnPoints.Errors, reu)
				mtx.Unlock()

				ks := "default"
				if v, ok := rcvMsg.Tags["ksid"]; ok {
					ks = v
				}
				statsPointsError(ks, "number")
				return
			}

			id := meta.ComposeID(m.GetKsid(), m.GetTsid())

			mtx.Lock()
			pts[i] = packet
			if _, ok := mm[id]; !ok {
				mm[id] = m
			}
			mtx.Unlock()

		}(rcvMsg, i)
	}

	wg.Wait()

	gerr := collect.persist.cluster.Write(pts)
	if gerr != nil {
		reu := RestErrorUser{
			Error: gerr.Message(),
		}
		returnPoints.Errors = append(returnPoints.Errors, reu)
	}

	go func() {
		mtx.Lock()
		defer mtx.Unlock()
		for ksts, m := range mm {

			if found := collect.boltc.Get(&ksts); found {
				statsProcTime(m.GetKsid(), time.Since(start), len(points))
				continue
			}

			ok, gerr := collect.cluster.Meta(&ksts, m)
			if gerr != nil {
				gblog.Error(
					fmt.Sprintf("%v", m),
					zap.String("func", "collector/HandlePoint"),
					zap.String("ksts", ksts),
					zap.Error(gerr),
				)
				statsProcTime(m.GetKsid(), time.Since(start), len(points))
				continue
			}
			if ok {
				collect.boltc.Set(ksts)
			}
			statsProcTime(m.GetKsid(), time.Since(start), len(points))
		}
	}()

	return returnPoints

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
