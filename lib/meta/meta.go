package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/utils"

	pb "github.com/uol/mycenae/lib/proto"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

type MetaData interface {
	Handle(pkt *pb.Meta) bool
	SaveTxtMeta(packet *pb.Meta)
	CheckTSID(esType, id string) (bool, gobol.Error)
}

type Meta struct {
	boltc    *bcache.Bcache
	validKey *regexp.Regexp
	settings *Settings
	persist  persistence

	concPoints  chan struct{}
	concBulk    chan struct{}
	metaPntChan chan *pb.Meta
	metaTxtChan chan *pb.Meta
	metaPayload *bytes.Buffer

	metas []*pb.Meta

	receivedSinceLastProbe int64
	errorsSinceLastProbe   int64
	saving                 int64
	shutdown               bool
	headInterval           time.Duration
	maxHead                int
	concHead               chan struct{}
}

type Settings struct {
	MetaSaveInterval    string
	MaxConcurrentBulks  int
	MaxConcurrentPoints int
	MaxMetaBulkSize     int
	MetaBufferSize      int
	MetaHeadInterval    string
}

func New(
	log *zap.Logger,
	sts *tsstats.StatsTS,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	set *Settings,
) (*Meta, error) {

	d, err := time.ParseDuration(set.MetaSaveInterval)
	if err != nil {
		return nil, err
	}
	hd, err := time.ParseDuration(set.MetaHeadInterval)
	if err != nil {
		return nil, err
	}

	gblog = log
	stats = sts

	m := &Meta{
		boltc:       bc,
		settings:    set,
		validKey:    regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		concPoints:  make(chan struct{}, set.MaxConcurrentPoints),
		concBulk:    make(chan struct{}, set.MaxConcurrentBulks),
		metaPntChan: make(chan *pb.Meta, set.MetaBufferSize),
		metaTxtChan: make(chan *pb.Meta, set.MetaBufferSize),
		metaPayload: &bytes.Buffer{},
		persist: persistence{
			esearch: es,
		},
		metas:        []*pb.Meta{},
		headInterval: hd,
		maxHead:      10 * set.MaxConcurrentBulks,
		concHead:     make(chan struct{}, 10*set.MaxConcurrentBulks),
	}

	gblog.Debug(
		"meta initialized",
		zap.String("MetaSaveInterval", set.MetaSaveInterval),
		zap.Int("MaxConcurrentBulks", set.MaxConcurrentBulks),
		zap.Int("MaxConcurrentPoints", set.MaxConcurrentPoints),
		zap.Int("MaxMetaBulkSize", set.MaxMetaBulkSize),
		zap.Int("MetaBufferSize", set.MetaBufferSize),
	)

	m.metaCoordinator(d, hd)

	return m, nil
}

func (meta *Meta) metaCoordinator(saveInterval time.Duration, headInterval time.Duration) {

	go func() {
		ticker := time.NewTicker(saveInterval)

		for {
			select {
			case <-ticker.C:

				if meta.metaPayload.Len() != 0 {

					meta.concBulk <- struct{}{}

					bulk := &bytes.Buffer{}

					err := meta.readMeta(bulk)
					if err != nil {
						gblog.Error(
							"",
							zap.String("func", "metaCoordinator"),
							zap.Error(err),
						)
						continue
					}

					go meta.saveBulk(bulk)
				}

			case p := <-meta.metaPntChan:

				gerr := meta.generateBulk(p, true)
				if gerr != nil {
					gblog.Error(
						gerr.Error(),
						zap.String("func", "metaCoordinator/SaveBulkES"),
					)
				}
				meta.metas = append(meta.metas, p)

				if meta.metaPayload.Len() > meta.settings.MaxMetaBulkSize {

					meta.concBulk <- struct{}{}

					bulk := &bytes.Buffer{}

					err := meta.readMeta(bulk)
					if err != nil {
						gblog.Error(
							"",
							zap.String("func", "metaCoordinator"),
							zap.Error(err),
						)
						continue
					}

					go meta.saveBulk(bulk)
				}

			case p := <-meta.metaTxtChan:

				gerr := meta.generateBulk(p, false)
				if gerr != nil {
					gblog.Error(
						gerr.Error(),
						zap.String("func", "metaCoordinator/SaveBulkES"),
					)
				}

				if meta.metaPayload.Len() > meta.settings.MaxMetaBulkSize {

					meta.concBulk <- struct{}{}

					bulk := &bytes.Buffer{}

					err := meta.readMeta(bulk)
					if err != nil {
						gblog.Error(
							"",
							zap.String("func", "metaCoordinator"),
							zap.Error(err),
						)
						continue
					}

					go meta.saveBulk(bulk)

				}
			}
		}
	}()
}

func (meta *Meta) head() {

	if len(meta.concHead) >= meta.maxHead {
		gblog.Warn(
			"max number of goroutines for head achived",
			zap.String("package", "meta"),
			zap.String("struct", "Meta"),
			zap.String("func", "head"),
			zap.Int("max", meta.maxHead),
		)
		return
	}

	meta.concHead <- struct{}{}

	go func() {
		mts := meta.metas
		meta.metas = []*pb.Meta{}
		for _, m := range mts {
			ksts := string(utils.KSTS(m.GetKsid(), m.GetTsid()))
			found, gerr := meta.CheckTSID("meta", ksts)
			if gerr != nil {
				gblog.Error(
					gerr.Error(),
					zap.String("package", "meta"),
					zap.String("struct", "Meta"),
					zap.String("func", "head"),
					zap.Error(gerr),
				)
			}
			if !found {
				meta.metaPntChan <- m
			}
			time.Sleep(meta.headInterval)
		}
		<-meta.concHead
	}()

}

func (meta *Meta) readMeta(bulk *bytes.Buffer) error {

	for {
		b, err := meta.metaPayload.ReadBytes(124)
		if err != nil {
			return err
		}

		b = b[:len(b)-1]

		_, err = bulk.Write(b)
		if err != nil {
			return err
		}

		if bulk.Len() >= meta.settings.MaxMetaBulkSize || meta.metaPayload.Len() == 0 {
			break
		}
		if meta.metaPayload.Len() <= 124 {
			meta.metaPayload.Reset()
		}
	}

	return nil
}

func (meta *Meta) Handle(pkt *pb.Meta) bool {

	ksts := string(utils.KSTS(pkt.GetKsid(), pkt.GetTsid()))
	if meta.boltc.Get(ksts) {
		return true
	}

	meta.boltc.Set(ksts)
	meta.metaPntChan <- pkt

	return true
}

func (meta *Meta) SaveTxtMeta(packet *pb.Meta) {

	ksts := utils.KSTS(packet.GetKsid(), packet.GetTsid())

	if len(meta.metaTxtChan) >= meta.settings.MetaBufferSize {
		gblog.Warn(
			fmt.Sprintf("discarding point: %v", packet),
			zap.String("package", "meta"),
			zap.String("func", "SaveMeta"),
		)
		statsLostMeta()
		return
	}
	found, gerr := meta.boltc.GetTsText(string(ksts), meta.CheckTSID)
	if gerr != nil {
		gblog.Error(
			gerr.Error(),
			zap.String("func", "saveMeta"),
			zap.Error(gerr),
		)

		atomic.AddInt64(&meta.errorsSinceLastProbe, 1)
	}

	if !found {
		meta.metaTxtChan <- packet
		statsBulkPoints()
	}

}

func (meta *Meta) generateBulk(packet *pb.Meta, number bool) gobol.Error {

	var metricType, tagkType, tagvType, metaType string

	if number {
		metricType = "metric"
		tagkType = "tagk"
		tagvType = "tagv"
		metaType = "meta"
	} else {
		metricType = "metrictext"
		tagkType = "tagktext"
		tagvType = "tagvtext"
		metaType = "metatext"
	}

	idx := BulkType{
		ID: EsIndex{
			EsIndex: packet.GetKsid(),
			EsType:  metricType,
			EsID:    packet.GetMetric(),
		},
	}

	indexJSON, err := json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	meta.metaPayload.Write(indexJSON)
	meta.metaPayload.WriteString("\n")

	metric := EsMetric{
		Metric: packet.GetMetric(),
	}

	docJSON, err := json.Marshal(metric)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	meta.metaPayload.Write(docJSON)
	meta.metaPayload.WriteString("\n")

	cleanTags := []Tag{}

	for _, tag := range packet.GetTags() {

		if tag.GetKey() != "ksid" && tag.GetKey() != "ttl" {

			idx := BulkType{
				ID: EsIndex{
					EsIndex: packet.GetKsid(),
					EsType:  tagkType,
					EsID:    tag.GetKey(),
				},
			}

			indexJSON, err := json.Marshal(idx)

			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(indexJSON)
			meta.metaPayload.WriteString("\n")

			docTK := EsTagKey{
				Key: tag.GetKey(),
			}

			docJSON, err := json.Marshal(docTK)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(docJSON)
			meta.metaPayload.WriteString("\n")

			idx = BulkType{
				ID: EsIndex{
					EsIndex: packet.GetKsid(),
					EsType:  tagvType,
					EsID:    tag.GetValue(),
				},
			}

			indexJSON, err = json.Marshal(idx)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(indexJSON)
			meta.metaPayload.WriteString("\n")

			docTV := EsTagValue{
				Value: tag.GetValue(),
			}

			docJSON, err = json.Marshal(docTV)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(docJSON)
			meta.metaPayload.WriteString("\n")

			cleanTags = append(cleanTags, Tag{
				Key:   tag.GetKey(),
				Value: tag.GetValue(),
			})

		}
	}

	idx = BulkType{
		ID: EsIndex{
			EsIndex: packet.GetKsid(),
			EsType:  metaType,
			EsID:    packet.GetTsid(),
		},
	}

	indexJSON, err = json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	meta.metaPayload.Write(indexJSON)
	meta.metaPayload.WriteString("\n")

	docM := MetaInfo{
		ID:     packet.GetTsid(),
		Metric: packet.GetMetric(),
		Tags:   cleanTags,
	}

	docJSON, err = json.Marshal(docM)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	meta.metaPayload.Write(docJSON)
	meta.metaPayload.WriteString("\n")

	meta.metaPayload.WriteString("|")

	return nil
}

func (meta *Meta) saveBulk(boby io.Reader) {

	gerr := meta.persist.SaveBulkES(boby)
	if gerr != nil {
		gblog.Error(
			gerr.Error(),
			zap.String("func", "metaCoordinator/SaveBulkES"),
		)
	}

	<-meta.concBulk
	meta.head()
}

func (meta *Meta) CheckTSID(esType, id string) (bool, gobol.Error) {

	info := strings.Split(id, "|")

	respCode, gerr := meta.persist.HeadMetaFromES(info[0], esType, info[1])
	if gerr != nil {
		return false, gerr
	}
	if respCode != 200 {
		return false, nil
	}

	return true, nil
}
