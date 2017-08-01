package wal

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
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
type WAL struct {
	id       int64
	created  int64
	stopCh   chan chan struct{}
	writeCh  chan *pb.TSPoint
	syncCh   chan []pb.TSPoint
	fd       *os.File
	mtx      sync.Mutex
	get      chan []pb.TSPoint
	give     chan []pb.TSPoint
	wg       sync.WaitGroup
	settings *Settings
	tt       tt
}

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

// New returns a WAL
func New(settings *Settings, l *zap.Logger) (*WAL, error) {

	logger = l

	si, err := time.ParseDuration(settings.SyncInterval)
	if err != nil {
		return nil, err
	}

	ckpti, err := time.ParseDuration(settings.CheckPointInterval)
	if err != nil {
		return nil, err
	}

	cli, err := time.ParseDuration(settings.CleanupInterval)
	if err != nil {
		return nil, err
	}

	settings.syncInterval = si
	settings.checkPointInterval = ckpti
	settings.cleanupInterval = cli

	wal := &WAL{
		settings: settings,
		stopCh:   make(chan chan struct{}),
		writeCh:  make(chan *pb.TSPoint, settings.MaxBufferSize),
		syncCh:   make(chan []pb.TSPoint, settings.MaxConcWrite),
		tt:       tt{table: make(map[string]int64)},
	}

	wal.get, wal.give = wal.recycler()

	if err := os.MkdirAll(settings.PathWAL, 0777); err != nil {
		return nil, err
	}

	names, err := wal.listFiles()

	if len(names) > 0 {
		lastWal := names[len(names)-1]
		id, err := idFromFileName(lastWal)
		if err != nil {
			return nil, err
		}

		wal.id = id
		stat, err := os.Stat(lastWal)
		if err != nil {
			return nil, err
		}

		if stat.Size() == 0 {
			os.Remove(lastWal)
			names = names[:len(names)-1]
		}
	}

	if err := wal.newFile(); err != nil {
		return nil, err
	}

	return wal, err

}

// Start dispatchs a goroutine with a ticker
// to save and sync points in disk
func (wal *WAL) Start() {

	wal.sync()

	for i := 0; i < wal.settings.MaxConcWrite; i++ {
		wal.worker()
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
		buffer := make([]pb.TSPoint, maxBufferSize)
		buffTimer := time.Now()
		index := 0

		for {
			select {
			case pt := <-wal.writeCh:
				if index >= maxBufferSize {
					wal.write(buffer[:index])
					index = 0
				}

				buffer[index] = *pt
				buffTimer = time.Now()
				index++

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
							buffer[index] = *pt
						} else {
							buffer[index] = *pt
							index++
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
func (wal *WAL) Add(p *pb.TSPoint) {
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

func (wal *WAL) makeBuffer() []pb.TSPoint {
	return make([]pb.TSPoint, wal.settings.MaxBufferSize)
}

type queued struct {
	when  time.Time
	slice []pb.TSPoint
}

func (wal *WAL) recycler() (get, give chan []pb.TSPoint) {

	get = make(chan []pb.TSPoint)
	give = make(chan []pb.TSPoint)

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

func (wal *WAL) write(pts []pb.TSPoint) {

	buffer := <-wal.get
	copy(buffer, pts)

	wal.syncCh <- buffer
}

func (wal *WAL) sync() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				if err := wal.fd.Sync(); err != nil {
					logger.Sugar().Errorf("error sycing data to commitlog: %v", err)
				} else {
					logger.Sugar().Debugf("%05d-%s synced", wal.id, fileSuffixName)
				}
			}
		}
	}()

	go func() {
		b := new(bytes.Buffer)
		for {
			buffer := <-wal.syncCh
			wal.wg.Add(1)

			b.Reset()

			encoder := gob.NewEncoder(b)

			err := encoder.Encode(buffer)
			if err != nil {
				logger.Sugar().Errorf("error creating buffer to be saved at commitlog: %v", err)
				wal.give <- buffer
				wal.wg.Done()
				continue
			}

			encodePts := make([]byte, len(buffer))
			compressed := snappy.Encode(encodePts, b.Bytes())
			b.Reset()
			size := make([]byte, offset)
			size[0] = logTypePoints
			binary.BigEndian.PutUint32(size[1:], uint32(len(compressed)))
			date := make([]byte, 8)
			binary.BigEndian.PutUint64(date, uint64(time.Now().Unix()))

			if _, err := wal.fd.Write(date); err != nil {
				logger.Sugar().Errorf("error writing date to commitlog: %v", err)
				wal.give <- buffer
				wal.wg.Done()
				continue
			}

			if _, err := wal.fd.Write(size); err != nil {
				logger.Sugar().Errorf("error writing header to commitlog: %v", err)
				wal.give <- buffer
				wal.wg.Done()
				continue
			}

			if _, err := wal.fd.Write(compressed); err != nil {
				logger.Sugar().Errorf("error writing data to commitlog: %v", err)
				wal.give <- buffer
				wal.wg.Done()
				continue
			}

			stat, err := wal.fd.Stat()
			if err != nil {
				logger.Sugar().Errorf("error doing stat at commitlog: %v", err)
				wal.give <- buffer
				wal.wg.Done()
				continue
			}

			if stat.Size() > maxFileSize {
				err = wal.newFile()
				if err != nil {
					logger.Sugar().Errorf("error creating new commitlog: %v", err)
				}
			}
			wal.give <- buffer
			wal.wg.Done()

		}

	}()

}

// idFromFileName parses the file ID from its name.
func idFromFileName(name string) (int64, error) {
	fileNameParts := strings.Split(filepath.Base(name), "-")
	if len(fileNameParts) < 2 {
		return 0, fmt.Errorf("%s has wrong format name", name)
	}
	return strconv.ParseInt(fileNameParts[0], 10, 32)
}

// newFile will close the current file and open a new one
func (wal *WAL) newFile() error {
	if wal.fd != nil {
		if err := wal.fd.Sync(); err != nil {
			return err
		}
		if err := wal.fd.Close(); err != nil {
			return err
		}
	}

	wal.id++
	fileName := filepath.Join(
		wal.settings.PathWAL,
		fmt.Sprintf("%05d-%s", wal.id, fileSuffixName),
	)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	wal.created = time.Now().Unix()
	wal.fd = fd

	return nil
}

func (wal *WAL) listFiles() ([]string, error) {

	names, err := filepath.Glob(
		filepath.Join(
			wal.settings.PathWAL,
			fmt.Sprintf("*-%s", fileSuffixName),
		))

	sort.Strings(names)

	return names, err

}
func (wal *WAL) Load() <-chan []pb.TSPoint {

	ptsChan := make(chan []pb.TSPoint)

	go func() {
		defer close(ptsChan)

		log := logger.With(
			zap.String("package", "wal"),
			zap.String("func", "Load"),
		)

		date, tt, err := wal.loadCheckpoint()
		if err != nil {
			log.Panic(
				"impossible to recovery checkpoint...",
				zap.Int64("check_point_date", date),
				zap.Error(err),
			)
		}

		//log.Sugar().Debug("transaction table loaded: ", tt)
		wal.tt.mtx.Lock()
		for k, v := range tt {
			wal.tt.table[k] = v
		}
		wal.tt.mtx.Unlock()

		names, err := wal.listFiles()
		if err != nil {
			log.Panic(
				"error getting list of files: %v",
				zap.Error(err),
			)
		}

		fCount := len(names) - 1
		if fCount < 1 {
			log.Debug("no wal to load")
			return
		}

		log.Debug("files to load", zap.Strings("list", names))

		time.Sleep(15 * time.Second)

		rp := make([]pb.TSPoint, wal.settings.MaxBufferSize)

		currentLog := filepath.Join(
			wal.settings.PathWAL,
			fmt.Sprintf("%05d-%s", wal.id, fileSuffixName),
		)
		for {

			filepath := names[fCount]
			if filepath == currentLog {
				fCount--
				if fCount < 0 {
					break
				}
				continue
			}

			log.Info(
				"loading wal",
				zap.String("file", filepath),
			)

			fileData, err := ioutil.ReadFile(filepath)
			if err != nil {
				log.Error("error reading file",
					zap.String("file", filepath),
					zap.Error(err),
				)
				continue
			}

			for {
				fileData = fileData[8:]
				typeSize := fileData[:offset]
				length := binary.BigEndian.Uint32(typeSize[1:])
				fileData = fileData[offset:]

				if len(fileData) < int(length) {
					log.Error("unable to read data from file, sizes don't match")
					break
				}

				decLen, err := snappy.DecodedLen(fileData[:length])
				if err != nil {
					log.Error(
						"decoding header",
						zap.String("file", filepath),
						zap.Uint32("bytes", length),
						zap.Error(err),
					)
					break
				}
				buf := make([]byte, decLen)

				data, err := snappy.Decode(buf, fileData[:length])
				if err != nil {
					log.Error(
						"decoding data",
						zap.String("file", filepath),
						zap.Uint32("bytes", length),
						zap.Error(err),
					)
					break
				}

				fileData = fileData[length:]

				buffer := bytes.NewBuffer(data)

				decoder := gob.NewDecoder(buffer)

				pts := []pb.TSPoint{}

				if err := decoder.Decode(&pts); err != nil {
					log.Error(
						"decoding points",
						zap.String("file", filepath),
						zap.Uint32("bytes", length),
						zap.Error(err),
					)
					break
				}

				var index int
				for _, p := range pts {
					if p.GetDate() > 0 {
						ksts := string(utils.KSTS(p.GetKsid(), p.GetTsid()))

						if utils.BlockID(p.GetDate()) > tt[ksts] {
							rp[index] = p
							index++
						}

					}

				}

				ptsChan <- rp[:index]

				log.Info(
					"loading points from wal",
					zap.String("file", filepath),
					zap.Int("count", index),
					zap.Int("data_lenght", len(fileData)),
				)

				if len(fileData) < offset {
					break
				}

			}

			fCount--
			if fCount < 0 {
				log.Debug("no more wal files to load")
				break
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

				names, err := wal.listFiles()
				if err != nil {
					logger.Error("Error getting list of files", zap.Error(err))
				}

				for _, f := range names {

					stat, err := os.Stat(f)
					if err != nil {
						logger.Sugar().Errorf("error to stat file %v: %v", f, err)
					}

					if !stat.ModTime().UTC().After(timeout) {
						logger.Sugar().Infof("removing write-ahead file %v", f)
						err = os.Remove(f)
						if err != nil {
							logger.Sugar().Errorf("error to remove file %v: %v", f, err)
						}
					}
				}

			}
		}

	}()

}
