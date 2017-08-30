package udpError

import (
	"time"

	"github.com/uol/gobol"
)

type ErrorInfo struct {
	ID      string    `json:"id"`
	Error   string    `json:"error"`
	Message string    `json:"message"`
	Date    time.Time `json:"date"`
}

type EsResponseTag struct {
	Took     int                  `json:"took"`
	TimedOut bool                 `json:"timed_out"`
	Shards   EsRespShards         `json:"_shards"`
	Hits     EsRespHitsWrapperTag `json:"hits"`
}

type EsRespHitsWrapperTag struct {
	Total    int             `json:"total"`
	MaxScore float32         `json:"max_score"`
	Hits     []EsRespHitsTag `json:"hits"`
}

type EsRespHitsTag struct {
	Index   string  `json:"_index"`
	Type    string  `json:"_type"`
	Id      string  `json:"_id"`
	Version int     `json:"_version,omitempty"`
	Found   bool    `json:"found,omitempty"`
	Score   float32 `json:"_score"`
	Source  TagKey  `json:"_source"`
}

type EsRespShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type TagKey struct {
	Key string `json:"key"`
}

type QueryWrapper struct {
	Size   int64       `json:"size,omitempty"`
	From   int64       `json:"from,omitempty"`
	Query  BoolWrapper `json:"filter"`
	Fields []string    `json:"fields,omitempty"`
}

type BoolWrapper struct {
	Bool OperatorWrapper `json:"bool"`
}

type OperatorWrapper struct {
	Must    []interface{} `json:"must,omitempty"`
	MustNot []interface{} `json:"must_not,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

type EsRegexp struct {
	Regexp map[string]string `json:"regexp"`
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type StructV2Error struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tagsError"`
}

func (s *StructV2Error) Validate() gobol.Error {
	return nil
}

type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}
