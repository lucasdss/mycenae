package wal

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/utils"
)

var (
	logger *zap.Logger
)

const (
	maxFileSize    = 64 * 1024 * 1024
	fileSuffixName = "waf.log"
	fileFlush      = "flush.log"
	checkPointName = "checkpoint.log"

	offset            = 5
	logTypePoints     = 0
	logTypeCheckpoint = 1
)

// WAL - Write-Ahead-Log
// Mycenae uses write-after-log, we save the point in memory
// and after a couple seconds at the log file.
/*
type WAL struct {
	id       int64
	created  int64
	stopCh   chan chan struct{}
	writeCh  chan *pb.Point
	syncCh   chan []pb.Point
	fd       *os.File
	mtx      sync.Mutex
	get      chan []pb.Point
	give     chan []pb.Point
	wg       sync.WaitGroup
	settings *Settings
	tt       tt
}
*/

type Settings struct {
	PathWAL         string
	SyncInterval    string
	syncInterval    time.Duration
	CleanupInterval string
	cleanupInterval time.Duration

	CheckPointInterval string
	checkPointInterval time.Duration
	CheckPointPath     string

	MaxBufferSize int
	MaxConcWrite  int
}

type tt struct {
	mtx   sync.RWMutex
	save  bool
	table map[string]int64
}

// Start dispatchs a goroutine with a ticker
// to save and sync points in disk
func (wal *WAL) Start() {
	err := wal.Open()
	if err != nil {
		logger.Sugar().Panicf("error to open wal file: %v", err)
	}

	for i := 0; i < wal.settings.MaxConcWrite; i++ {
		wal.worker()
		wal.syncWorker()
	}

	wal.checkpoint()
	wal.cleanup()

}

func (wal *WAL) Stop() {

	var wg sync.WaitGroup

	for i := 0; i < wal.settings.MaxConcWrite; i++ {
		wg.Add(1)
		go func() {
			ch := make(chan struct{})
			wal.stopCh <- ch
			<-ch
			wg.Done()
		}()
	}

	wg.Wait()

	if wal.fd != nil {
		wal.fd.Sync()
		if err := wal.fd.Close(); err != nil {
			logger.Sugar().Errorf("error closing commitlog: %v", err)
		}
	}
}

func (wal *WAL) worker() {
	go func() {
		maxBufferSize := wal.settings.MaxBufferSize
		si := wal.settings.syncInterval
		ticker := time.NewTicker(500 * time.Millisecond)
		buffer := make([]pb.Point, maxBufferSize)
		buffTimer := time.Now()
		index := 0

		for {
			select {
			case pt := <-wal.writeCh:
				if index >= maxBufferSize {
					wal.write(buffer[:index])
					index = 0
				}

				if pt != nil {
					buffer[index] = *pt
					buffTimer = time.Now()
					index++
				}

			case <-ticker.C:
				if time.Now().Sub(buffTimer) > si && index > 0 {
					wal.write(buffer[:index])
					index = 0
				}

			case ch := <-wal.stopCh:
				if len(wal.writeCh) > 0 {
					for pt := range wal.writeCh {
						if index >= maxBufferSize {
							wal.write(buffer[:index])
							index = 0
							if pt != nil {
								buffer[index] = *pt
							}
						} else {
							if pt != nil {
								buffer[index] = *pt
								index++
							}
						}

						if len(wal.writeCh) == 0 {
							close(wal.writeCh)
							break
						}
					}
				}

				if index > 0 {
					wal.write(buffer[:index])
				}
				wal.wg.Wait()

				ch <- struct{}{}

				return

			}

		}
	}()
}

// Add append point at the end of the file
func (wal *WAL) Add(p *pb.Point) {
	wal.writeCh <- p
}

func (wal *WAL) SetTT(ksts string, date int64) {
	wal.tt.mtx.Lock()
	defer wal.tt.mtx.Unlock()

	if date > wal.tt.table[ksts] {
		wal.tt.table[ksts] = date
		wal.tt.save = true
	}
}

func (wal *WAL) DeleteTT(ksts string) {
	wal.tt.mtx.Lock()
	defer wal.tt.mtx.Unlock()
	delete(wal.tt.table, ksts)
}

func (wal *WAL) checkpoint() {

	go func() {

		ticker := time.NewTicker(wal.settings.checkPointInterval)
		fileName := filepath.Join(wal.settings.CheckPointPath, checkPointName)

		for {

			select {
			case <-ticker.C:

				wal.tt.mtx.Lock()
				if !wal.tt.save {
					wal.tt.mtx.Unlock()
					continue
				}
				wal.tt.save = false
				wal.tt.mtx.Unlock()

				date := make([]byte, 8)
				binary.BigEndian.PutUint64(date, uint64(time.Now().Unix()))

				wal.tt.mtx.RLock()
				tt, err := json.Marshal(wal.tt.table)
				wal.tt.mtx.RUnlock()
				if err != nil {
					logger.Sugar().Errorf("error creating transaction table buffer: %v", err)
					continue
				}
				sizeTT := make([]byte, 4)
				binary.BigEndian.PutUint32(sizeTT, uint32(len(tt)))

				buf := make([]byte, len(date)+len(sizeTT)+len(tt))
				copy(buf, date)
				copy(buf[len(date):], sizeTT)
				copy(buf[len(date)+len(sizeTT):], tt)

				err = ioutil.WriteFile(fileName, buf, 0664)
				if err != nil {
					logger.Sugar().Errorf("unable to write to file %v: %v", fileName, err)
				}

			}
		}

	}()
}

func (wal *WAL) makeBuffer() []pb.Point {
	return make([]pb.Point, wal.settings.MaxBufferSize)
}

type queued struct {
	when  time.Time
	slice []pb.Point
}

func (wal *WAL) recycler() (get, give chan []pb.Point) {

	get = make(chan []pb.Point)
	give = make(chan []pb.Point)

	go func() {
		q := new(list.List)
		for {
			if q.Len() == 0 {
				q.PushFront(queued{when: time.Now(), slice: wal.makeBuffer()})
			}

			e := q.Front()

			timeout := time.NewTimer(time.Minute)
			select {
			case b := <-give:
				timeout.Stop()
				q.PushFront(queued{when: time.Now(), slice: b})

			case get <- e.Value.(queued).slice:
				timeout.Stop()
				q.Remove(e)

			case <-timeout.C:
				e := q.Front()
				for e != nil {
					n := e.Next()
					if time.Since(e.Value.(queued).when) > time.Minute {
						q.Remove(e)
						e.Value = nil
					}
					e = n
				}
			}
		}

	}()

	return

}

func (wal *WAL) write(pts []pb.Point) {

	buffer := <-wal.get
	copy(buffer, pts)

	wal.syncCh <- buffer
}

func (wal *WAL) syncWorker() {

	go func() {

		for {
			buffer := <-wal.syncCh
			wal.wg.Add(1)

			valuesMap := make(map[string][]Value)

			for _, p := range buffer {
				ksts := string(utils.KSTS(p.GetKsid(), p.GetTsid()))

				valuesMap[ksts] = append(valuesMap[ksts], NewFloatValue(p.GetDate(), float64(p.GetValue())))

				/*
					logger.Debug(
						"values to be writen",
						zap.Int64("date", p.GetDate()),
						zap.Float32("value", p.GetValue()),
						zap.Any("values", valuesMap[ksts]),
						zap.Int("sizeValues", len(valuesMap)),
						zap.Int("count", count),
					)
				*/
			}

			segID, err := wal.WriteMulti(valuesMap)
			if err != nil {
				logger.Error(
					err.Error(),
					zap.String("package", "wal"),
					zap.String("func", "syncWorker"),
					zap.Error(err),
					zap.Int64("segID", segID),
				)
			}

			wal.give <- buffer
			wal.wg.Done()

		}

	}()

}

func (wal *WAL) Load() <-chan []*pb.Point {

	ptsChan := make(chan []*pb.Point, wal.settings.MaxConcWrite)

	go func() {
		defer close(ptsChan)

		log := logger.With(
			zap.String("package", "wal"),
			zap.String("func", "Load"),
		)

		date, tt, err := wal.loadCheckpoint()
		if err != nil {
			log.Error(
				"impossible to recovery checkpoint...",
				zap.Int64("check_point_date", date),
				zap.Error(err),
			)
			return
		}

		wal.tt.mtx.Lock()
		for k, v := range tt {
			wal.tt.table[k] = v
		}
		wal.tt.mtx.Unlock()

		names, err := segmentFileNames(wal.settings.PathWAL)
		if err != nil {
			log.Error(
				"error getting list of files",
				zap.Error(err),
			)
			return
		}

		for _, fn := range names {
			if err := func() error {
				f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
				if err != nil {
					return err
				}

				// Log some information about the segments.
				stat, err := os.Stat(f.Name())
				if err != nil {
					return err
				}
				log.Info(
					"reading file",
					zap.String("file", f.Name()),
					zap.Int64("size", stat.Size()),
				)

				r := NewWALSegmentReader(f)
				defer r.Close()

				for r.Next() {
					entry, err := r.Read()
					if err != nil {
						n := r.Count()
						log.Info("file corrupt, truncating",
							zap.String("file", f.Name()),
							zap.Int64("position", n),
						)
						if err := f.Truncate(n); err != nil {
							return err
						}
						break
					}

					switch t := entry.(type) {
					case *WriteWALEntry:

						pts := []*pb.Point{}
						for ksts, values := range t.Values {
							x := strings.Split(ksts, "|")
							ksid := x[0]
							tsid := x[1]

							if len(ksid) < 1 || len(tsid) < 1 {
								continue
							}

							for _, v := range values {

								pD := v.UnixNano()
								pV := float32(v.Value().(float64))

								if utils.BlockID(pD) >= tt[ksts] {
									pts = append(
										pts,
										&pb.Point{
											Date:  pD,
											Value: pV,
											Ksid:  ksid,
											Tsid:  tsid,
										})
								}
							}
						}

						ptsChan <- pts

					case *DeleteRangeWALEntry:
						log.Info("DeleteRangeWALEntry")

					case *DeleteWALEntry:
						log.Info("DeleteWALEntry")

					}
				}

				return nil
			}(); err != nil {
				log.Error("unable to read wal files",
					zap.Strings("files", names),
					zap.Error(err),
				)
				return
			}
		}

		return
	}()

	return ptsChan
}

func (wal *WAL) loadCheckpoint() (int64, map[string]int64, error) {

	fileName := filepath.Join(wal.settings.CheckPointPath, checkPointName)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return time.Now().Unix(), map[string]int64{}, nil
	}

	checkPointData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, nil, err
	}

	date := int64(binary.BigEndian.Uint64(checkPointData[:8]))

	//ttSize := binary.BigEndian.Uint32(checkPointData[8:12])
	//logger.Sugar().Debug("ttSize ", ttSize)

	var tt map[string]int64
	err = json.Unmarshal(checkPointData[12:], &tt)
	return date, tt, err

}

func (wal *WAL) cleanup() {

	go func() {
		ticker := time.NewTicker(wal.settings.cleanupInterval)
		for {
			select {
			case <-ticker.C:

				timeout := time.Now().UTC().Add(-2 * time.Hour)
				var lst []string

				names, err := segmentFileNames(wal.settings.PathWAL)
				if err != nil {
					logger.Error(
						err.Error(),
						zap.String("func", "cleanup"),
						zap.String("struct", "wal"),
						zap.Error(err),
					)
				}

				for _, f := range names {

					stat, err := os.Stat(f)
					if err != nil {
						logger.Error(err.Error(),
							zap.String("func", "cleanup"),
							zap.String("struct", "wal"),
							zap.String("file", f),
							zap.Error(err),
						)
					}

					if !stat.ModTime().UTC().After(timeout) {
						lst = append(lst, f)
					}
				}

				err = wal.Remove(lst)
				if err != nil {
					logger.Error(
						"error after removed wal",
						zap.String("func", "cleanup"),
						zap.String("struct", "wal"),
						zap.Error(err),
					)
				}

			}
		}

	}()

}
