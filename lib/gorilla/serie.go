package gorilla

import (
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	"go.uber.org/zap"
)

type serie struct {
	mtx        sync.RWMutex
	ksid       string
	tsid       string
	blocks     [maxBlocks]*block
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

	t.index = getIndex(now)
	t.saveIdx = -1

	blkTime := now

	blkid := BlockID(blkTime)
	i := getIndex(blkid)

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

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/addPoint"),
	)

	delta := int(p.GetDate() - t.blocks[t.index].id)

	if delta >= bucketSize {
		t.lastWrite = now

		t.saveIdx = t.index
		t.cleanup = true

		t.index = getIndex(p.GetDate())

		blkid := BlockID(p.GetDate())
		if t.blocks[t.index] == nil {
			log.Debug(
				"new block",
				zap.Int64("blkid", blkid),
			)
			t.blocks[t.index] = &block{id: blkid}
			t.blocks[t.index].add(p)
			return nil
		}

		if t.blocks[t.index].id != blkid {
			log.Debug(
				"resetting block",
				zap.Int64("blkid", blkid),
			)
			t.store(t.index)
			t.blocks[t.index].reset(blkid)
			t.blocks[t.index].add(p)
			return nil
		}

		log.Debug(
			"updating block",
			zap.Int64("blkid", blkid),
		)

		gerr := t.update(p)
		return gerr

	}

	if delta < 0 {
		t.lastWrite = now
		return t.update(p)
	}

	t.lastWrite = now
	t.blocks[t.index].add(p)

	//log.Debug("point written successfully")
	return nil
}

func (t *serie) toDepot() bool {
	t.mtx.RLock()
	ksid := t.ksid
	tsid := t.tsid
	idx := t.index
	lw := t.lastWrite
	la := t.lastAccess
	saveIdx := t.saveIdx
	cleanup := t.cleanup
	t.mtx.RUnlock()

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
		t.mtx.Lock()
		t.saveIdx = -1
		t.mtx.Unlock()
		return false
	}

	if delta >= hour {
		log.Info("sending serie to depot")
		go t.store(idx)
	}

	if cleanup {
		if now-la >= hour {
			log.Info("cleanup serie")
			t.mtx.Lock()
			for i := 0; i < maxBlocks; i++ {
				if t.index == i {
					continue
				}
				t.blocks[i] = nil
			}
			t.cleanup = false
			t.mtx.Unlock()
		}
	}

	if now-la >= hour && now-lw >= hour {
		log.Info("serie must leave memory")
		return true
	}

	return false
}

func (t *serie) stop() gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.saveIdx >= 0 {
		go t.store(t.saveIdx)
	}
	return t.store(t.index)
}

func (t *serie) update(p *pb.TSPoint) gobol.Error {

	blkID := BlockID(p.GetDate())

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", blkID),
		zap.Int64("pointDate", p.GetDate()),
		zap.Float32("pointValue", p.GetValue()),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/update"),
	)

	index := getIndex(blkID)

	var pByte []byte
	if t.blocks[index] != nil && t.blocks[index].id == blkID {
		pByte = t.blocks[index].GetPoints()
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
	}

	var pts [bucketSize]*pb.Point
	var count int
	if len(pByte) >= headerSize {
		points, c, gerr := t.decode(pByte, blkID)
		if gerr != nil {
			log.Error(
				gerr.Error(),
				zap.Error(gerr),
			)
			return gerr
		}
		pts = points
		count = c
	}

	delta := p.GetDate() - blkID

	log.Debug(
		"point delta",
		zap.Int64("delta", delta),
	)

	if pts[delta] == nil {
		count++
	}
	pts[delta] = &pb.Point{Date: p.GetDate(), Value: p.GetValue()}

	ptsByte, gerr := t.encode(pts[:], blkID)
	if gerr != nil {
		log.Error(
			gerr.Error(),
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
			zap.Error(gerr),
		)
		return gerr
	}

	gerr = t.persist.Write(t.ksid, t.tsid, blkID, ptsByte)
	if gerr != nil {
		log.Error(
			gerr.Error(),
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
			zap.Error(gerr),
		)
		return gerr
	}

	if t.blocks[index] != nil && t.blocks[index].id == blkID {
		log.Debug(
			"updating block in memory",
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
		)

		t.blocks[index].SetPoints(ptsByte)
	}

	log.Debug(
		"block updated",
		zap.Int64("delta", delta),
		zap.Int("blockSize", len(ptsByte)),
		zap.Int("count", count),
	)

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
	if oi >= maxBlocks {
		oi = 0
	}

	var ot int64
	if t.blocks[oi] != nil {
		ot = t.blocks[oi].id
	} else {
		ot = t.blocks[t.index].id - (22 * hour)
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
	blkidEnd := BlockID(end)
	for {
		blkidStart := BlockID(x)
		blksID = append(blksID, blkidStart)

		x += 2 * hour
		if blkidStart >= blkidEnd {
			break
		}
	}

	var memPts []*pb.Point
	if end > ot {
		ptsCh := make(chan query)
		defer close(ptsCh)

		for x, b := range blksID {
			i := getIndex(b)
			if t.blocks[i] != nil {
				go t.blocks[i].rangePoints(x, start, end, ptsCh)
			} else {
				pByte, gerr := t.persist.Read(t.ksid, t.tsid, b)
				if gerr != nil {
					log.Error(
						gerr.Error(),
						zap.Error(gerr),
					)
					return nil, gerr
				}
				t.blocks[i] = &block{id: b, points: pByte}
				go t.blocks[i].rangePoints(x, start, end, ptsCh)
			}

		}

		result := make([][]*pb.Point, len(blksID))
		var resultCount int
		for _ = range blksID {
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
		var i int
		for _ = range blksID {

			if len(result[i]) > 0 {
				copy(points[size:], result[i])
				size += len(result[i])
			}
			i++

		}

		memPts = points
	}

	pts := make([]*pb.Point, totalCount)
	copy(pts, oldPts)
	copy(pts[len(oldPts):], memPts)

	return pts, nil
}

func (t *serie) readPersistence(start, end int64) ([]*pb.Point, gobol.Error) {

	oldBlocksID := []int64{}

	x := start

	blkidEnd := BlockID(end)
	for {
		blkidStart := BlockID(x)
		oldBlocksID = append(oldBlocksID, blkidStart)

		x += 2 * hour
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

	pts, err := enc.Close()
	if err != nil {
		log.Error(
			err.Error(),
			zap.Error(err),
			zap.Int("blockSize", len(pts)),
			zap.Int("count", count),
		)
		return nil, errTsz("serie/encode", t.ksid, t.tsid, 0, err)
	}

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

	if err := dec.Close(); err != nil {
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

	if t.blocks[index] == nil {
		return nil
	}

	bktid := t.blocks[index].id
	pts := t.blocks[index].GetPoints()

	if len(pts) >= headerSize {
		gblog.Debug(
			"writting block to depot",
			zap.String("package", "gorilla"),
			zap.String("func", "serie/store"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", bktid),
			zap.Int("index", index),
		)
		err := t.persist.Write(t.ksid, t.tsid, bktid, pts)
		if err != nil {
			gblog.Error(
				"",
				zap.String("package", "gorilla"),
				zap.String("func", "serie/store"),
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bktid),
				zap.Int("index", index),
				zap.Error(err),
			)
			return err
		}
		gblog.Debug(
			"block persisted",
			zap.String("package", "gorilla"),
			zap.String("func", "serie/store"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", bktid),
			zap.Int("index", index),
		)
	}

	return nil
}
