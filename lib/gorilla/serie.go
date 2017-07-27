package gorilla

import (
	"io"
	"sync"
	"time"

	"github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/utils"
	"go.uber.org/zap"
)

type serie struct {
	mtx        sync.RWMutex
	mtxDepot   sync.Mutex
	ksid       string
	tsid       string
	blocks     [utils.MaxBlocks]*block
	index      int
	saveIdx    int
	lastWrite  int64
	lastAccess int64
	cleanup    bool
	persist    depot.Persistence
}

type query struct {
	id  int
	pts []*pb.Point
}

func newSerie(persist depot.Persistence, ksid, tsid string) *serie {

	s := &serie{
		ksid:       ksid,
		tsid:       tsid,
		lastWrite:  time.Now().Unix(),
		lastAccess: time.Now().Unix(),
		persist:    persist,
		blocks:     [12]*block{},
	}

	s.init()

	return s
}

func (t *serie) init() {

	log := gblog.With(
		zap.String("package", "gorilla"),
		zap.String("func", "serie/init"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
	)

	t.mtx.Lock()
	defer t.mtx.Unlock()

	log.Debug("initializing serie")

	now := time.Now().Unix()

	t.index = utils.GetIndex(now)
	t.saveIdx = -1

	blkid := utils.BlockID(now)
	i := utils.GetIndex(blkid)

	t.blocks[i] = &block{id: blkid}

	blkPoints, err := t.persist.Read(t.ksid, t.tsid, blkid)
	if err != nil {
		log.Error(
			"error to initialize block",
			zap.Int64("blkid", blkid),
			zap.Error(err),
		)
		return
	}

	t.blocks[i].SetPoints(blkPoints)

}

func (t *serie) addPoint(p *pb.TSPoint) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	now := time.Now().Unix()

	delta := int(p.GetDate() - t.blocks[t.index].id)

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int("index", t.index),
		zap.Int64("blkid", t.blocks[t.index].id),
		zap.Int("delta", delta),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/addPoint"),
	)

	if delta >= bucketSize {
		t.lastWrite = now

		t.saveIdx = t.index
		t.cleanup = true

		t.index = utils.GetIndex(p.GetDate())
		blkid := utils.BlockID(p.GetDate())
		if t.blocks[t.index] == nil {
			log.Debug(
				"new block",
				zap.Int("new_index", t.index),
				zap.Int64("new_blkid", blkid),
			)
			t.blocks[t.index] = &block{id: blkid}
			t.blocks[t.index].Add(p)
			return nil
		}

		if t.blocks[t.index].id != blkid {
			log.Debug(
				"resetting block",
				zap.Int("new_index", t.index),
				zap.Int64("old_blkid", t.blocks[t.index].id),
				zap.Int64("new_blkid", blkid),
			)
			t.store(t.index)
			t.blocks[t.index].Reset(blkid)
			t.blocks[t.index].Add(p)
			return nil
		}

		log.Debug(
			"adding point to a new block",
			zap.Int("new_index", t.index),
			zap.Int64("new_blkid", blkid),
		)

		t.blocks[t.index].Add(p)

		return nil

	}

	if delta < 0 {
		t.lastWrite = now
		return t.update(p)
	}

	t.lastWrite = now
	t.blocks[t.index].Add(p)

	//log.Debug("point written successfully")
	return nil
}

func (t *serie) toDepot() bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	ksid := t.ksid
	tsid := t.tsid
	idx := t.index
	lw := t.lastWrite
	la := t.lastAccess
	saveIdx := t.saveIdx
	cleanup := t.cleanup

	now := time.Now().Unix()
	delta := now - lw

	log := gblog.With(
		zap.String("ksid", ksid),
		zap.String("tsid", tsid),
		zap.Int64("lastWrite", lw),
		zap.Int64("lastAccess", la),
		zap.Int("index", idx),
		zap.Int64("delta", delta),
		zap.Bool("cleanup", cleanup),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/toDepot"),
	)

	if saveIdx >= 0 {
		go t.store(saveIdx)
		if t.saveIdx == saveIdx {
			t.saveIdx = -1
		}
		return false
	}

	if cleanup {
		if now-la >= utils.Hour {
			log.Info("cleanup serie")
			for i := 0; i < utils.MaxBlocks; i++ {
				if idx != i {
					t.blocks[i] = nil
				}
			}
			t.cleanup = false
		}
	}

	if delta >= utils.Hour {
		log.Info("sending serie to depot")
		go t.store(idx)
	}

	if now-la >= utils.Hour && now-lw >= utils.Hour {
		log.Info("serie must leave memory")
		return true
	}

	return false
}

func (t *serie) stop() (int64, gobol.Error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.saveIdx >= 0 {
		go t.store(t.saveIdx)
	}

	return t.blocks[t.index].id, t.store(t.index)
}

func (t *serie) update(p *pb.TSPoint) gobol.Error {
	t.mtxDepot.Lock()
	defer t.mtxDepot.Unlock()

	blkID := utils.BlockID(p.GetDate())

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", blkID),
		zap.Int64("pointDate", p.GetDate()),
		zap.Float32("pointValue", p.GetValue()),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/update"),
	)

	index := utils.GetIndex(blkID)

	var pByte []byte
	var blk *block
	if t.blocks[index] != nil && t.blocks[index].id == blkID {

		pByte = t.blocks[index].GetPoints()
		blk = t.blocks[index]

	} else {

		x, gerr := t.persist.Read(t.ksid, t.tsid, blkID)
		if gerr != nil {
			log.Error(
				gerr.Error(),
				zap.Error(gerr),
			)
			return gerr
		}

		pByte = x
		blk = &block{id: blkID}

	}

	err := blk.NewEncoder(pByte, p.GetDate(), p.GetValue())
	if err != nil {
		return errTsz("serie/update", t.ksid, t.tsid, 0, err)
	}

	gerr := t.persist.Write(t.ksid, t.tsid, blkID, blk.GetPoints())
	if gerr != nil {
		return gerr
	}

	log.Debug("block updated")

	return nil

}

func (t *serie) read(start, end int64) ([]*pb.Point, gobol.Error) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	now := time.Now().Unix()
	t.lastAccess = now
	t.cleanup = true

	// Oldest Index
	oi := t.index + 1
	if oi >= utils.MaxBlocks {
		oi = 0
	}

	var ot int64
	if t.blocks[oi] != nil {
		ot = t.blocks[oi].id
	} else {
		ot = t.blocks[t.index].id - (22 * utils.Hour)
		// calc old time
	}

	log := gblog.With(
		zap.String("package", "storage/serie"),
		zap.String("func", "read"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("index", t.index),
		zap.Int("oldestIndex", oi),
		zap.Int64("oldestTime", ot),
	)

	memStart := start
	var oldPts []*pb.Point
	if start < ot {
		memStart = ot
		pEnd := ot
		if end < ot || end > now {
			pEnd = end
		}
		p, err := t.readPersistence(start, pEnd)
		if err != nil {
			return nil, err
		}
		if len(p) > 0 {
			oldPts = p
		}
		log.Debug(
			"points read from depot",
			zap.Int("persistenceCount", len(oldPts)),
		)
	}

	totalCount := len(oldPts)

	blksID := []int64{}

	x := memStart
	blkidEnd := utils.BlockID(end)

	for {
		blkidStart := utils.BlockID(x)
		blksID = append(blksID, blkidStart)

		x += 2 * utils.Hour
		if blkidStart >= blkidEnd {
			break
		}
	}

	var memPts []*pb.Point
	if end > ot {
		ptsCh := make(chan query)
		defer close(ptsCh)

		for x, b := range blksID {
			i := utils.GetIndex(b)
			if t.blocks[i] == nil {
				pByte, gerr := t.persist.Read(t.ksid, t.tsid, b)
				if gerr != nil {
					log.Error(
						gerr.Error(),
						zap.Error(gerr),
					)
					return nil, gerr
				}
				t.blocks[i] = &block{id: b, points: pByte}
			}
			go t.blocks[i].rangePoints(x, start, end, ptsCh)
		}

		result := make([][]*pb.Point, len(blksID))
		var resultCount int
		for range blksID {
			q := <-ptsCh
			result[q.id] = q.pts
			size := len(result[q.id])
			if size > 0 {
				resultCount += size
			}
		}

		points := make([]*pb.Point, resultCount)
		totalCount += resultCount

		var size int
		// index must be from oldest point to the newest
		for i := range blksID {

			if len(result[i]) > 0 {
				copy(points[size:], result[i])
				size += len(result[i])
			}
		}

		memPts = points
	}

	pts := make([]*pb.Point, totalCount)
	copy(pts, oldPts)
	copy(pts[len(oldPts):], memPts)

	log.Debug("read from serie")

	return pts, nil
}

func (t *serie) readPersistence(start, end int64) ([]*pb.Point, gobol.Error) {

	oldBlocksID := []int64{}

	x := start

	blkidEnd := utils.BlockID(end)

	for {
		blkidStart := utils.BlockID(x)
		oldBlocksID = append(oldBlocksID, blkidStart)

		x += 2 * utils.Hour
		if blkidStart >= blkidEnd {
			break
		}
	}

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/readPersistence"),
		zap.Int("blocksCount", len(oldBlocksID)),
	)

	log.Debug("reading...")

	var pts []*pb.Point
	for _, blkid := range oldBlocksID {

		log.Debug(
			"reading from persistence",
			zap.Int64("blockID", blkid),
		)

		pByte, err := t.persist.Read(t.ksid, t.tsid, blkid)
		if err != nil {
			return nil, err
		}

		if len(pByte) >= headerSize {

			p, _, err := t.decode(pByte, blkid)
			if err != nil {
				return nil, err
			}

			for i, np := range p {
				if np != nil {
					if np.Date >= start && np.Date <= end {
						pts = append(pts, np)
					}
					log.Debug(
						"point from persistence",
						zap.Int64("blockID", blkid),
						zap.Int64("pointDate", np.Date),
						zap.Float32("pointValue", np.Value),
						zap.Int("rangeIdx", i),
					)
				}
			}
		}
	}

	return pts, nil

}

func (t *serie) encode(points []*pb.Point, id int64) ([]byte, gobol.Error) {

	log := gblog.With(
		zap.String("package", "storage/serie"),
		zap.String("func", "encode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", id),
	)

	enc := tsz.NewEncoder(id)
	var count int

	for _, pt := range points {
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
			count++
		}
	}

	// tsz.Encoder.Close() always returns nil
	pts, _ := enc.Close()

	log.Debug(
		"finished tsz encoding",
		zap.Int("blockSize", len(pts)),
		zap.Int("count", count),
	)

	return pts, nil
}

func (t *serie) decode(points []byte, id int64) ([bucketSize]*pb.Point, int, gobol.Error) {
	dec := tsz.NewDecoder(points)

	var pts [bucketSize]*pb.Point
	var d int64
	var v float32
	var count int

	log := gblog.With(
		zap.String("package", "storage"),
		zap.String("func", "serie/decode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", id),
		zap.Int("blockSize", len(points)),
	)

	for dec.Scan(&d, &v) {
		delta := d - id
		if delta >= 0 && delta < bucketSize {
			pts[delta] = &pb.Point{Date: d, Value: v}
			count++
		}
	}

	err := dec.Close()
	if err != nil && err != io.EOF {
		log.Error(
			err.Error(),
			zap.Error(err),
			zap.Int("count", count),
		)
		return [bucketSize]*pb.Point{}, 0, errTsz("serie/decode", t.ksid, t.tsid, 0, err)
	}

	log.Debug(
		"finished tsz decoding",
		zap.Int("count", count),
	)

	return pts, count, nil

}

func (t *serie) store(index int) gobol.Error {
	t.mtxDepot.Lock()
	defer t.mtxDepot.Unlock()

	if t.blocks[index] == nil {
		return nil
	}

	bktid := t.blocks[index].id
	pts := t.blocks[index].GetPoints()

	if len(pts) >= headerSize {
		log := gblog.With(
			zap.String("package", "gorilla"),
			zap.String("func", "serie/store"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", bktid),
			zap.Int("index", index),
		)

		log.Debug("writting block to depot")

		err := t.persist.Write(t.ksid, t.tsid, bktid, pts)
		if err != nil {
			log.Error(err.Error(), zap.Error(err))
			return err
		}

		log.Debug("block persisted")
	}

	return nil
}
