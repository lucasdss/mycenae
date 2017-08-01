package gorilla

import (
	"sync"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/wal"

	"github.com/uol/mycenae/lib/utils"
	"go.uber.org/zap"
)

var (
	gblog      *zap.Logger
	stats      *tsstats.StatsTS
	headerSize = utils.HeaderSize
)

// Storage keeps all timeseries in memory
// after a while the serie will be saved at cassandra
// if the time range is not in memory it must query cassandra
type Storage struct {
	persist depot.Persistence
	stop    chan chan struct{}
	tsmap   map[string]*serie
	localTS localTSmap
	dump    chan struct{}
	wal     *wal.WAL
	mtx     sync.RWMutex
}

type localTSmap struct {
	mtx   sync.RWMutex
	tsmap map[string]Meta
}

type Meta struct {
	KSID      string
	TSID      string
	LastCheck int64
}

// New returns Storage
func New(
	lgr *zap.Logger,
	sts *tsstats.StatsTS,
	persist depot.Persistence,
	w *wal.WAL,
) *Storage {

	stats = sts
	gblog = lgr

	return &Storage{
		stop:    make(chan chan struct{}),
		tsmap:   make(map[string]*serie),
		localTS: localTSmap{tsmap: make(map[string]Meta)},
		dump:    make(chan struct{}),
		wal:     w,
		persist: persist,
	}
}

func (s *Storage) Stop() {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	gblog.Debug(
		"flushing all timeseries",
		zap.String("func", "Stop"),
		zap.String("package", "gorilla"),
	)

	c := make(chan struct{})
	s.stop <- c
	<-c

	gblog.Debug(
		"flushed all timeseries",
		zap.String("func", "Stop"),
		zap.String("package", "gorilla"),
	)

	s.wal.Stop()

}

// Start dispatch a goroutine to save buckets
// in cassandra. All buckets with more than an hour
// must be compressed and saved in cassandra.
func (s *Storage) Start() {
	go func() {
		ticker := time.NewTicker(time.Minute)

		for {
			select {
			case <-ticker.C:
				now := time.Now().Unix()

				for _, serie := range s.ListSeries() {
					delta := now - serie.LastCheck
					if delta > 1800 {
						// we need a way to persist ts older than 2h
						// after 26h the serie must be out of memory
						s.updateLastCheck(&serie)
						if s.getSerie(serie.KSID, serie.TSID).toDepot() {
							s.deleteSerie(serie.KSID, serie.TSID)
						}
					}
				}
			case stpC := <-s.stop:

				s.mtx.Lock()

				u := make(map[string]int64)
				var wg sync.WaitGroup
				for id, ls := range s.tsmap {

					wg.Add(1)
					go func(id string, ls *serie, wg *sync.WaitGroup) {
						defer wg.Done()

						blkid, err := ls.stop()
						if err != nil {
							gblog.Error(
								"unable to save serie",
								zap.String("ksid", ls.ksid),
								zap.String("tsid", ls.tsid),
								zap.Error(err),
							)

							u[id] = blkid

						} else {
							gblog.Debug("saved", zap.String("ksid", ls.ksid), zap.String("tsid", ls.tsid))
						}

					}(id, ls, &wg)

				}
				wg.Wait()

				stpC <- struct{}{}
				return

			}
		}
	}()
}

func (s *Storage) ListSeries() []Meta {
	s.localTS.mtx.RLock()
	defer s.localTS.mtx.RUnlock()

	m := []Meta{}
	for _, meta := range s.localTS.tsmap {
		m = append(m, meta)
	}
	return m
}

func (s *Storage) updateLastCheck(serie *Meta) {

	id := s.id(serie.KSID, serie.TSID)
	s.localTS.mtx.Lock()
	defer s.localTS.mtx.Unlock()

	serie.LastCheck = time.Now().Unix()
	s.localTS.tsmap[id] = *serie
}

func (s *Storage) Delete(m Meta) <-chan []*pb.Point {

	ptsC := make(chan []*pb.Point)

	now := time.Now().Unix()

	start := utils.BlockID(now)

	go func() {
		defer close(ptsC)

		pts, err := s.getSerie(m.KSID, m.TSID).read(start, now)
		if err != nil {
			gblog.Error(
				"",
				zap.String("package", "gorilla"),
				zap.String("func", "storage/Delete"),
				zap.Error(err),
			)
			return
		}

		ptsC <- pts
		s.deleteSerie(m.KSID, m.TSID)

	}()

	return ptsC
}

//Add new point in a timeseries
func (s *Storage) Write(p *pb.TSPoint) gobol.Error {
	s.wal.Add(p)

	return s.getSerie(p.GetKsid(), p.GetTsid()).addPoint(p)
}

func (s *Storage) WAL(p *pb.TSPoint) gobol.Error {
	return s.getSerie(p.Ksid, p.Tsid).addPoint(p)
}

//Read points from a timeseries, if range start bigger than 24hours
// it will read points from persistence
func (s *Storage) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {
	return s.getSerie(ksid, tsid).read(start, end)
}

func (s *Storage) getSerie(ksid, tsid string) *serie {
	s.mtx.RLock()
	id := s.id(ksid, tsid)
	serie := s.tsmap[id]
	s.mtx.RUnlock()

	if serie == nil {
		s.mtx.Lock()
		serie = s.tsmap[id]
		if serie == nil {
			serie = newSerie(s.persist, ksid, tsid)
			s.tsmap[id] = serie
		}
		s.mtx.Unlock()

		s.localTS.mtx.Lock()
		s.localTS.tsmap[id] = Meta{
			KSID:      ksid,
			TSID:      tsid,
			LastCheck: time.Now().Unix(),
		}
		s.localTS.mtx.Unlock()

	}
	return serie
}

func (s *Storage) deleteSerie(ksid, tsid string) {

	ksts := string(utils.KSTS(ksid, tsid))
	gblog.Info(
		"removing serie from memory",
		zap.String("ksid", ksid),
		zap.String("tsid", tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "deleteSerie"),
	)

	s.mtx.Lock()
	delete(s.tsmap, ksts)
	s.mtx.Unlock()

	s.localTS.mtx.Lock()
	delete(s.localTS.tsmap, ksts)
	s.localTS.mtx.Unlock()

	s.wal.DeleteTT(ksts)

}

func (s *Storage) id(ksid, tsid string) string {

	return string(utils.KSTS(ksid, tsid))
}
