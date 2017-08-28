package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/bcache"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/utils"
	"go.uber.org/zap"
)

const (
	elasticMetaType   = "meta"
	elasticNestedPath = "tagsNested"
)

type elasticMeta struct {
	boltc    *bcache.Bcache
	validKey *regexp.Regexp
	settings *structs.MetaSettings
	esearch  *rubber.Elastic
	stats    *tsstats.StatsTS
	logger   *zap.Logger

	concBulk    chan struct{}
	metaPntChan chan *pb.Meta
	metaTxtChan chan *pb.Meta
	metaPayload *bytes.Buffer

	sm *savingObj

	receivedSinceLastProbe int64
	errorsSinceLastProbe   int64
	saving                 int64
	shutdown               bool
}

type savingObj struct {
	mm  map[string]*pb.Meta
	mtx sync.RWMutex
}

func (so *savingObj) get(key string) (*pb.Meta, bool) {
	so.mtx.RLock()
	defer so.mtx.RUnlock()
	v, ok := so.mm[key]
	return v, ok
}

func (so *savingObj) add(key string, m *pb.Meta) {
	so.mtx.Lock()
	defer so.mtx.Unlock()
	so.mm[key] = nil
}

func (so *savingObj) del(key string) {
	so.mtx.Lock()
	defer so.mtx.Unlock()
	delete(so.mm, key)
}

func (so *savingObj) iter() <-chan string {
	c := make(chan string)
	go func() {
		so.mtx.RLock()
		for k := range so.mm {
			so.mtx.RUnlock()
			c <- k
			so.mtx.RLock()
		}
		so.mtx.RUnlock()
		close(c)
	}()
	return c
}

func createElasticMeta(
	log *zap.Logger,
	sts *tsstats.StatsTS,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	set *structs.MetaSettings,
) (*elasticMeta, error) {
	d, err := time.ParseDuration(set.MetaSaveInterval)
	if err != nil {
		return nil, err
	}
	hd, err := time.ParseDuration(set.MetaHeadInterval)
	if err != nil {
		return nil, err
	}

	m := &elasticMeta{
		boltc:       bc,
		settings:    set,
		esearch:     es,
		validKey:    regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		concBulk:    make(chan struct{}, set.MaxConcurrentBulks),
		metaPntChan: make(chan *pb.Meta, set.MetaBufferSize),
		metaTxtChan: make(chan *pb.Meta, set.MetaBufferSize),
		metaPayload: bytes.NewBuffer(nil),
		stats:       sts,
		logger:      log,
		sm:          &savingObj{mm: make(map[string]*pb.Meta)},
	}

	m.logger.Debug(
		"meta initialized",
		zap.String("MetaSaveInterval", set.MetaSaveInterval),
		zap.Int("MaxConcurrentBulks", set.MaxConcurrentBulks),
		zap.Int("MaxConcurrentPoints", set.MaxConcurrentPoints),
		zap.Int("MaxMetaBulkSize", set.MaxMetaBulkSize),
		zap.Int("MetaBufferSize", set.MetaBufferSize),
	)

	go m.metaCoordinator(d, hd)

	return m, nil
}

func (meta *elasticMeta) metaCoordinator(saveInterval time.Duration, headInterval time.Duration) {
	go func() {
		ticker := time.NewTicker(saveInterval)
		for {
			select {
			case <-ticker.C:
				for ksts := range meta.sm.iter() {
					//found, gerr := meta.boltc.GetTsNumber(ksts, meta.CheckTSID)
					found, gerr := meta.CheckTSID(elasticMetaType, ksts)
					if gerr != nil {
						meta.logger.Error(
							gerr.Error(),
							zap.String("func", "metaCoordinator"),
							zap.Error(gerr),
						)
						continue
					}
					if !found {
						if pkt, ok := meta.sm.get(ksts); ok {
							meta.metaPntChan <- pkt
							time.Sleep(headInterval)
							continue
						}
					}

					if gerr := meta.boltc.Set(ksts); gerr != nil {
						meta.logger.Error(
							gerr.Error(),
							zap.String("func", "metaCoordinator"),
							zap.Error(gerr),
						)
					}
					meta.sm.del(ksts)
					time.Sleep(headInterval)
				}
			}
		}
	}()

	ticker := time.NewTicker(saveInterval)

	for {
		select {
		case <-ticker.C:
			if meta.metaPayload.Len() != 0 {
				meta.concBulk <- struct{}{}
				bulk := bytes.NewBuffer(nil)
				err := meta.readMeta(bulk)
				if err != nil {
					meta.logger.Error(
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
				meta.logger.Error(
					gerr.Error(),
					zap.String("func", "metaCoordinator/SaveBulkES"),
				)
			}
			if meta.metaPayload.Len() > meta.settings.MaxMetaBulkSize {
				meta.concBulk <- struct{}{}
				bulk := bytes.NewBuffer(nil)
				err := meta.readMeta(bulk)
				if err != nil {
					meta.logger.Error(
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
				meta.logger.Error(
					gerr.Error(),
					zap.String("func", "metaCoordinator/SaveBulkES"),
				)
			}
			if meta.metaPayload.Len() > meta.settings.MaxMetaBulkSize {
				meta.concBulk <- struct{}{}
				bulk := bytes.NewBuffer(nil)
				err := meta.readMeta(bulk)
				if err != nil {
					meta.logger.Error(
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
}

func (meta *elasticMeta) readMeta(bulk *bytes.Buffer) error {
	for {
		b, err := meta.metaPayload.ReadBytes(124) // |
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
	}
	return nil
}

func (meta *elasticMeta) Handle(pkt *pb.Meta) bool {
	ksts := utils.KSTS(pkt.GetKsid(), pkt.GetTsid())
	if meta.boltc.Get(ksts) {
		return true
	}

	if _, ok := meta.sm.get(string(ksts)); !ok {
		meta.logger.Debug(
			"adding point in save map",
			zap.String("package", packageName),
			zap.String("func", "Handle"),
			zap.String("ksts", string(ksts)),
		)
		meta.sm.add(string(ksts), pkt)
		meta.metaPntChan <- pkt
	}
	return false
}

func (meta *elasticMeta) SaveTxtMeta(packet *pb.Meta) {
	ksts := utils.KSTS(packet.GetKsid(), packet.GetTsid())

	if len(meta.metaTxtChan) >= meta.settings.MetaBufferSize {
		meta.logger.Warn(
			fmt.Sprintf("discarding point: %v", packet),
			zap.String("package", packageName),
			zap.String("func", "SaveMeta"),
		)
		statsLostMeta(meta.stats)
		return
	}
	found, gerr := meta.boltc.GetTsText(string(ksts), meta.CheckTSID)
	if gerr != nil {
		meta.logger.Error(
			gerr.Error(),
			zap.String("func", "saveMeta"),
			zap.Error(gerr),
		)

		atomic.AddInt64(&meta.errorsSinceLastProbe, 1)
	}

	if !found {
		meta.metaTxtChan <- packet
		statsBulkPoints(meta.stats)
	}
}

func (meta *elasticMeta) generateBulk(packet *pb.Meta, number bool) gobol.Error {
	var content []byte
	var (
		metricType = "metrictext"
		tagkType   = "tagktext"
		tagvType   = "tagvtext"
		metaType   = "metatext"
	)
	if number {
		metricType = "metric"
		tagkType = "tagk"
		tagvType = "tagv"
		metaType = elasticMetaType
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
			idx = BulkType{
				ID: EsIndex{
					EsIndex: packet.GetKsid(),
					EsType:  tagkType,
					EsID:    tag.GetKey(),
				},
			}
			content, err = json.Marshal(idx)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(content)
			meta.metaPayload.WriteString("\n")
			docTK := EsTagKey{
				Key: tag.GetKey(),
			}

			content, err = json.Marshal(docTK)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			meta.metaPayload.Write(content)
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
	docM := Info{
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

func (meta *elasticMeta) saveBulk(body io.Reader) {
	defer func() { <-meta.concBulk }()
	start := time.Now()
	status, err := meta.esearch.PostBulk(body)
	if err != nil {
		statsIndexError(meta.stats, "", "", "bulk")
		meta.logger.Error(
			"Elastic search problem",
			zap.String("function", "saveBulk"),
			zap.String("structure", "elasticMeta"),
			zap.String("package", packageName),
			zap.Int("status", status),
			zap.Error(err),
		)
	}
	statsIndex(meta.stats, "", "", "bulk", time.Since(start))
}

func (meta *elasticMeta) CheckTSID(esType, id string) (bool, gobol.Error) {
	info := strings.Split(id, "|")
	esindex, id := info[0], info[1]

	start := time.Now()
	respCode, err := meta.esearch.GetHead(esindex, esType, id)
	if err != nil {
		statsIndexError(meta.stats, esindex, esType, "head")
		return false, errPersist("HeadMetaFromES", err)
	}
	statsIndex(meta.stats, esindex, esType, "head", time.Since(start))
	return respCode == http.StatusOK, nil
}

func (meta *elasticMeta) SendError(index, dtype, id string, doc ErrorData) gobol.Error {
	start := time.Now()
	_, err := meta.esearch.Put(index, dtype, id, doc)
	if err != nil {
		statsIndexError(meta.stats, index, dtype, "put")
		return errPersist("SendErrorToES", err)
	}
	statsIndex(meta.stats, index, dtype, "PUT", time.Since(start))
	return nil
}

func (meta *elasticMeta) CreateIndex(index string) gobol.Error {
	start := time.Now()
	body := bytes.NewBuffer(nil)
	body.WriteString(mappingIndex)
	_, err := meta.esearch.CreateIndex(index, body)
	if err != nil {
		statsIndexError(meta.stats, index, "", "post")
		return errPersist("CreateIndex", err)
	}
	statsIndex(meta.stats, index, "", "post", time.Since(start))
	return nil
}

func (meta *elasticMeta) DeleteIndex(index string) gobol.Error {
	start := time.Now()
	_, err := meta.esearch.DeleteIndex(index)
	if err != nil {
		statsIndexError(meta.stats, index, "", "delete")
		return errPersist("DeleteIndex", err)
	}

	statsIndex(meta.stats, index, "", "delete", time.Since(start))
	return nil
}
