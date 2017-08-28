package udpError

import (
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/meta"
)

type ErrorInfo struct {
	ID      string    `json:"id"`
	Error   string    `json:"error"`
	Message string    `json:"message"`
	Date    time.Time `json:"date"`
}

type StructV2Error struct {
	Key    string     `json:"key"`
	Metric string     `json:"metric"`
	Tags   []meta.Tag `json:"tagsError"`
}

func (s *StructV2Error) Validate() gobol.Error {
	return nil
}

type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}
