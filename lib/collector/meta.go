package collector

import (
	"bytes"
	"encoding/json"
	"io"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"

	"go.uber.org/zap"
)

func (collect *Collector) metaCoordinator(saveInterval time.Duration) {

	ticker := time.NewTicker(saveInterval)

	for {
		select {
		case <-ticker.C:

			if collect.metaPayload.Len() != 0 {

				collect.concBulk <- struct{}{}

				bulk := &bytes.Buffer{}

				err := collect.readMeta(bulk)
				if err != nil {
					gblog.Error(
						"",
						zap.String("func", "collector/metaCoordinator"),
						zap.Error(err),
					)
					continue
				}

				go collect.saveBulk(bulk)

			}

		case p := <-collect.metaChan:

			gerr := collect.generateBulk(p)
			if gerr != nil {
				gblog.Error(
					gerr.Error(),
					zap.String("func", "collector/metaCoordinator/SaveBulkES"),
				)
			}

			if collect.metaPayload.Len() > collect.settings.MaxMetaBulkSize {

				collect.concBulk <- struct{}{}

				bulk := &bytes.Buffer{}

				err := collect.readMeta(bulk)
				if err != nil {
					gblog.Error(
						"",
						zap.String("func", "collector/metaCoordinator"),
						zap.Error(err),
					)
					continue
				}

				go collect.saveBulk(bulk)
			}
		}
	}
}

func (collect *Collector) readMeta(bulk *bytes.Buffer) error {

	for {
		b, err := collect.metaPayload.ReadBytes(124)
		if err != nil {
			return err
		}

		b = b[:len(b)-1]

		_, err = bulk.Write(b)
		if err != nil {
			return err
		}

		if bulk.Len() >= collect.settings.MaxMetaBulkSize || collect.metaPayload.Len() == 0 {
			break
		}
	}

	return nil
}

func (collect *Collector) saveMeta(packet gorilla.Point) {

	found := false

	var gerr gobol.Error

	lid := len(packet.ID)
	lksid := len(packet.KsID)
	x := make([]byte, lid+lksid+1)
	copy(x, packet.KsID)
	copy(x[lksid:], "|")
	copy(x[lksid+1:], packet.ID)
	ksts := string(x)

	//ksts := fmt.Sprintf("%v|%v", packet.KsID, packet.ID)

	if packet.Number {
		found, gerr = collect.boltc.GetTsNumber(ksts, collect.CheckTSID)
	} else {
		found, gerr = collect.boltc.GetTsText(ksts, collect.CheckTSID)
	}
	if gerr != nil {
		gblog.Error(
			gerr.Error(),
			zap.String("func", "collector/saveMeta"),
			zap.Error(gerr),
		)

		atomic.AddInt64(&collect.errorsSinceLastProbe, 1)

	}

	if !found {
		collect.metaChan <- packet
		statsBulkPoints()
	}

}

func (collect *Collector) generateBulk(packet gorilla.Point) gobol.Error {

	var metricType, tagkType, tagvType, metaType string

	if packet.Number {
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
			EsIndex: packet.KsID,
			EsType:  metricType,
			EsID:    packet.Message.Metric,
		},
	}

	indexJSON, err := json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(indexJSON)
	collect.metaPayload.WriteString("\n")

	metric := EsMetric{
		Metric: packet.Message.Metric,
	}

	docJSON, err := json.Marshal(metric)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(docJSON)
	collect.metaPayload.WriteString("\n")

	cleanTags := []Tag{}

	for k, v := range packet.Message.Tags {

		if k != "ksid" && k != "ttl" {

			idx := BulkType{
				ID: EsIndex{
					EsIndex: packet.KsID,
					EsType:  tagkType,
					EsID:    k,
				},
			}

			indexJSON, err := json.Marshal(idx)

			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			collect.metaPayload.Write(indexJSON)
			collect.metaPayload.WriteString("\n")

			docTK := EsTagKey{
				Key: k,
			}

			docJSON, err := json.Marshal(docTK)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			collect.metaPayload.Write(docJSON)
			collect.metaPayload.WriteString("\n")

			idx = BulkType{
				ID: EsIndex{
					EsIndex: packet.KsID,
					EsType:  tagvType,
					EsID:    v,
				},
			}

			indexJSON, err = json.Marshal(idx)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			collect.metaPayload.Write(indexJSON)
			collect.metaPayload.WriteString("\n")

			docTV := EsTagValue{
				Value: v,
			}

			docJSON, err = json.Marshal(docTV)
			if err != nil {
				return errMarshal("saveTsInfo", err)
			}

			collect.metaPayload.Write(docJSON)
			collect.metaPayload.WriteString("\n")

			cleanTags = append(cleanTags, Tag{Key: k, Value: v})

		}
	}

	idx = BulkType{
		ID: EsIndex{
			EsIndex: packet.KsID,
			EsType:  metaType,
			EsID:    packet.ID,
		},
	}

	indexJSON, err = json.Marshal(idx)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(indexJSON)
	collect.metaPayload.WriteString("\n")

	docM := MetaInfo{
		ID:     packet.ID,
		Metric: packet.Message.Metric,
		Tags:   cleanTags,
	}

	docJSON, err = json.Marshal(docM)
	if err != nil {
		return errMarshal("saveTsInfo", err)
	}

	collect.metaPayload.Write(docJSON)
	collect.metaPayload.WriteString("\n")

	collect.metaPayload.WriteString("|")

	return nil
}

func (collect *Collector) saveBulk(boby io.Reader) {

	gerr := collect.persist.SaveBulkES(boby)
	if gerr != nil {
		gblog.Error(
			gerr.Error(),
			zap.String("func", "collector/metaCoordinator/SaveBulkES"),
		)
	}

	<-collect.concBulk
}
