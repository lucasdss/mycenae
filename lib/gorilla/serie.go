package gorilla

import (
	"fmt"
	"sync"
	"time"

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
		blocks:     [utils.MaxBlocks]*block{},
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

func (t *serie) addPoint(p *pb.Point) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	now := time.Now().Unix()

	delta := p.GetDate() - t.blocks[t.index].id

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int("index", t.index),
		zap.Int64("blkid", t.blocks[t.index].id),
		zap.Int64("delta", delta),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/addPoint"),
	)

	if delta >= bucketSize {
		t.lastWrite = now

		t.blocks[t.index].ToDepot(true)
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

		blkid := utils.BlockID(p.GetDate())
		index := utils.GetIndex(p.GetDate())

		if time.Unix(blkid, 0).Before(time.Now().Add(-22 * time.Hour)) {
			return t.update(p)
		}

		if t.blocks[index] == nil {
			pByte, gerr := t.persist.Read(t.ksid, t.tsid, blkid)
			if gerr != nil {
				return gerr
			}
			t.blocks[index] = &block{id: blkid, points: pByte}
			t.blocks[index].Add(p)
			t.blocks[index].ToDepot(true)
			return nil
		}

		if t.blocks[index].id == blkid {
			t.blocks[index].Add(p)
			t.blocks[index].ToDepot(true)
			return nil
		}

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

	var saving bool
	for i, blk := range t.blocks {
		if blk != nil && blk.SendToDepot() {
			go t.store(i)
			saving = true
		}
	}

	if saving {
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

	for i, blk := range t.blocks {
		if blk != nil && blk.SendToDepot() {
			go t.store(i)
		}
	}

	return t.blocks[t.index].id, t.store(t.index)
}

func (t *serie) update(p *pb.Point) gobol.Error {
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

	var blk *block
	if t.blocks[index] != nil && t.blocks[index].id == blkID {

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

		blk = &block{id: blkID, points: x}

	}

	blk.Add(p)

	gerr := t.persist.Write(t.ksid, t.tsid, blkID, blk.Close())
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

	var oldPts []*pb.Point
	if start < ot {
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

	var memPts []*pb.Point
	if end > ot {
		ptsCh := make(chan query)
		defer close(ptsCh)

		// oldest index in memory
		i := oi
		blkdid := ot
		for x := 0; x < utils.MaxBlocks; x++ {
			if t.blocks[i] == nil {
				pByte, gerr := t.persist.Read(t.ksid, t.tsid, blkdid)
				if gerr != nil {
					log.Error(
						gerr.Error(),
						zap.Error(gerr),
					)
					return nil, gerr
				}
				t.blocks[i] = &block{id: blkdid, points: pByte}
			}
			go t.blocks[i].rangePoints(i, start, end, ptsCh)

			blkdid += 2 * utils.Hour
			i++
			if i >= utils.MaxBlocks {
				i = 0
			}
		}

		result := make([][]*pb.Point, utils.MaxBlocks)
		var resultCount int
		for x := 0; x < utils.MaxBlocks; x++ {
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
		i = oi
		for x := 0; x < utils.MaxBlocks; x++ {
			if len(result[i]) > 0 {
				copy(points[size:], result[i])
				size += len(result[i])
			}
			i++
			if i >= utils.MaxBlocks {
				i = 0
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

			blk := &block{id: blkid}
			p, err := blk.decode(pByte)
			if err != nil {
				return nil, errTsz("serie/readPersistence", t.ksid, t.tsid, blkid, err)
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

func (t *serie) store(index int) gobol.Error {
	t.mtxDepot.Lock()
	defer t.mtxDepot.Unlock()

	bktid := t.blocks[index].id
	pts := t.blocks[index].GetPoints()

	if len(pts) >= headerSize {
		log := gblog.With(
			zap.String("package", "gorilla"),
			zap.String("func", "serie/store"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("bktid", bktid),
			zap.Int("index", index),
		)

		log.Debug("writting block to depot")

		err := t.persist.Write(t.ksid, t.tsid, bktid, pts)
		if err != nil {
			log.Error(err.Error(), zap.Error(err))
			return err
		}
		t.blocks[index].ToDepot(false)

		log.Debug("block persisted")
	}

	return nil
}

func (t *serie) Merge(blkid int64, pts []byte) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	index := utils.GetIndex(blkid)

	if t.blocks[index] == nil {
		gblog.Debug(
			"initializing block",
			zap.String("package", "gorilla"),
			zap.String("func", "serie/Merge"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", blkid),
			zap.Int("index", index),
		)
		t.blocks[index] = &block{id: blkid, points: pts}
		return nil
	}

	if t.blocks[index].id == blkid {
		gblog.Debug(
			"merging points",
			zap.String("package", "gorilla"),
			zap.String("func", "serie/Merge"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", blkid),
			zap.Int("index", index),
		)

		err := t.blocks[index].Merge(pts)

		return err
	}

	return fmt.Errorf("block in memory (%v) does not match block id (%v) to be merged", t.blocks[index].id, blkid)
}
