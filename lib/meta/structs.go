package meta

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"
)

type RestError struct {
	Datapoint gorilla.TSDBpoint `json:"datapoint"`
	Gerr      gobol.Error       `json:"error"`
}

type RestErrorUser struct {
	Datapoint gorilla.TSDBpoint `json:"datapoint"`
	Error     interface{}       `json:"error"`
}

type RestErrors struct {
	Errors  []RestErrorUser `json:"errors"`
	Failed  int             `json:"failed"`
	Success int             `json:"success"`
}

type StructV2Error struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tagsError"`
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

type LogMeta struct {
	Action string   `json:"action"`
	Meta   MetaInfo `json:"meta"`
}

type EsIndex struct {
	EsID    string `json:"_id"`
	EsType  string `json:"_type"`
	EsIndex string `json:"_index"`
}

type BulkType struct {
	ID EsIndex `json:"index"`
}

type EsMetric struct {
	Metric string `json:"metric"`
}

type EsTagKey struct {
	Key string `json:"key"`
}

type EsTagValue struct {
	Value string `json:"value"`
}
