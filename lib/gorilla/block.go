package gorilla

import (
	"fmt"
	"io"
	"sync"

	"go.uber.org/zap"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/utils"
)

const (
	bucketSize = 7200
)

// block contains compressed points
type block struct {
	mtx      sync.RWMutex
	toDepot  bool
	points   []byte
	id       int64
	prevDate int64
	enc      *tsz.Encoder
}

func (b *block) Reset(id int64) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	log := gblog.With(
		zap.String("package", "storage/block"),
		zap.String("func", "Add"),
		zap.Int64("blkid", b.id),
		zap.Int64("newBlkid", id),
	)

	b.id = id
	b.points = nil
	b.prevDate = 0
	if b.enc != nil {
		log.Debug("resetting block with open encoding")
		p, _ := b.enc.Close()
		log.Debug(
			"encoding closed",
			zap.Int("ptsSize", len(p)),
		)
	}
	b.enc = tsz.NewEncoder(b.id)
	return
}

func (b *block) Add(p *pb.Point) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	log := gblog.With(
		zap.String("package", "storage/block"),
		zap.String("func", "Add"),
		zap.Int64("blkid", b.id),
	)

	if b.enc == nil {
		if len(b.points) >= headerSize {
			err := b.newEncoder(b.points, p.GetDate(), p.GetValue())
			if err != nil {
				log.Error(
					err.Error(),
					zap.Error(err),
				)
			}
			return
		}
		b.enc = tsz.NewEncoder(b.id)
	}

	if p.GetDate() > b.prevDate {
		b.enc.Encode(p.GetDate(), p.GetValue())
		b.prevDate = p.GetDate()
		return
	}

	pBytes, _ := b.enc.Close()

	err := b.newEncoder(pBytes, p.GetDate(), p.GetValue())
	if err != nil {
		log.Error(
			"problem to transcode tsz",
			zap.Error(err),
			zap.Int("blockSize", len(pBytes)),
		)
	}
	return
}

func (b *block) close() []byte {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	log := gblog.With(
		zap.String("package", "storage/block"),
		zap.String("func", "close"),
		zap.Int64("blkid", b.id),
	)

	if b.enc != nil {
		pts, _ := b.enc.Close()
		b.points = pts
		b.enc = nil

		log.Debug(
			"points array closed",
			zap.Int("size", len(pts)),
		)

	}

	return b.points

}

func (b *block) NewEncoder(pByte []byte, date int64, value float32) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.newEncoder(pByte, date, value)
}

func (b *block) newEncoder(pByte []byte, date int64, value float32) error {
	log := gblog.With(
		zap.String("struct", "storage/block"),
		zap.String("func", "newEncoder"),
		zap.Int64("blkid", b.id),
	)

	var points [bucketSize]*pb.Point
	if len(pByte) >= headerSize {

		pts, err := b.decode(pByte)
		if err != nil {
			log.Error(
				err.Error(),
				zap.Error(err),
			)
			return err
		}
		points = pts
	}

	delta := date - b.id

	blkid := utils.BlockID(date)
	if blkid != b.id {
		log.Error(
			"block id divergency",
			zap.Int64("point_blkid", blkid),
			zap.Int64("delta", delta),
		)
		return fmt.Errorf("block id divergency, delta %v", delta)
	}

	log.Debug(
		"point delta",
		zap.Int64("delta", delta),
	)

	points[delta] = &pb.Point{Date: date, Value: value}

	enc := tsz.NewEncoder(b.id)
	for _, p := range points {
		if p != nil {
			enc.Encode(p.Date, p.Value)
			b.prevDate = p.Date
		}
	}

	b.enc = enc

	return nil
}

func (b *block) decode(points []byte) ([bucketSize]*pb.Point, error) {

	if len(points) < headerSize {
		return [bucketSize]*pb.Point{}, nil
	}

	id := b.id
	dec := tsz.NewDecoder(points)

	var pts [bucketSize]*pb.Point
	var d int64
	var v float32

	for dec.Scan(&d, &v) {
		delta := d - id
		pts[delta] = &pb.Point{Date: d, Value: v}
	}

	err := dec.Close()
	if err != nil && err != io.EOF {
		return [bucketSize]*pb.Point{}, err
	}

	return pts, nil
}

func (b *block) SetPoints(pts []byte) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(pts) >= headerSize {
		b.points = pts
	}
}

func (b *block) GetPoints() []byte {

	return b.close()
}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.Lock()
	points := b.points
	if b.enc != nil {
		points = b.enc.Get()
	}
	bktid := b.id
	b.mtx.Unlock()

	var pts []*pb.Point
	if len(points) >= headerSize {
		if end >= bktid || start >= bktid {

			dec := tsz.NewDecoder(points)

			var d int64
			var v float32
			for dec.Scan(&d, &v) {
				if d >= start && d <= end {
					pts = append(pts, &pb.Point{
						Date:  d,
						Value: v,
					})
				}
			}

			err := dec.Close()
			if err != nil && err != io.EOF {
				gblog.Error("", zap.Error(err))
			}

		}
	}

	queryCh <- query{
		id:  id,
		pts: pts,
	}

}

func (b *block) SendToDepot() bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.toDepot
}

func (b *block) ToDepot(tdp bool) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.toDepot = tdp
}

func (b *block) Merge(points []byte) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	pts1, err := b.decode(points)
	if err != nil {
		return err
	}

	enc := tsz.NewEncoder(b.id)

	if b.enc != nil {
		pts2, err := b.decode(b.close())
		if err != nil {
			return err
		}

		for i, p1 := range pts1 {

			p2 := pts2[i]
			if p2 != nil {
				enc.Encode(p2.Date, p2.Value)
				continue
			}
			if p1 != nil {
				enc.Encode(p1.Date, p1.Value)
				continue
			}
		}

		b.enc = enc
		b.points = enc.Get()

		return nil
	}

	if len(b.points) >= headerSize {
		pts2, err := b.decode(b.points)
		if err != nil {
			return err
		}

		for i, p1 := range pts1 {

			p2 := pts2[i]
			if p2 != nil {
				enc.Encode(p2.Date, p2.Value)
				continue
			}
			if p1 != nil {
				enc.Encode(p1.Date, p1.Value)
				continue
			}
		}

		b.enc = enc
		b.points = enc.Get()

		return nil

	}

	for _, p1 := range pts1 {
		if p1 != nil {
			enc.Encode(p1.Date, p1.Value)
		}
	}

	b.enc = enc
	b.points = enc.Get()

	return nil
}
