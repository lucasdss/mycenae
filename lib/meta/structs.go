package meta

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"
)

// RestError ...
type RestError struct {
	Datapoint gorilla.TSDBpoint `json:"datapoint"`
	Gerr      gobol.Error       `json:"error"`
}

// RestErrorUser ...
type RestErrorUser struct {
	Datapoint gorilla.TSDBpoint `json:"datapoint"`
	Error     interface{}       `json:"error"`
}

// RestErrors ...
type RestErrors struct {
	Errors  []RestErrorUser `json:"errors"`
	Failed  int             `json:"failed"`
	Success int             `json:"success"`
}

// StructV2Error ...
type StructV2Error struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tagsError"`
}

// Info ...
type Info struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

// LogMeta ...
type LogMeta struct {
	Action string `json:"action"`
	Meta   Info   `json:"meta"`
}

// EsIndex ...
type EsIndex struct {
	EsID    string `json:"_id"`
	EsType  string `json:"_type"`
	EsIndex string `json:"_index"`
}

// BulkType ....
type BulkType struct {
	ID EsIndex `json:"index"`
}

// EsMetric ...
type EsMetric struct {
	Metric string `json:"metric"`
}

// EsTagKey ...
type EsTagKey struct {
	Key string `json:"key"`
}

// EsTagValue ...
type EsTagValue struct {
	Value string `json:"value"`
}
