package plot

import (
	"regexp"
	"sort"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/utils"
)

var (
	validFields = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/]+$`)
	validFwild  = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/*]+$`)
	validFor    = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/|]+$`)
)

type TsQuery struct {
	Downsample Downsample       `json:"downsample"`
	Start      int64            `json:"start"`
	End        int64            `json:"end"`
	Keys       []Key            `json:"keys"`
	Merge      map[string]Merge `json:"merge"`
	Text       []Key            `json:"text"`
	TextSearch string           `json:"textSearch"`

	re *regexp.Regexp
}

func (query *TsQuery) Validate() gobol.Error {

	i, err := utils.MilliToSeconds(query.Start)
	if err != nil {
		return errValidationS("ListPoints", err.Error())
	}
	query.Start = i

	j, err := utils.MilliToSeconds(query.End)
	if err != nil {
		return errValidationS("ListPoints", err.Error())
	}
	query.End = j

	if query.End < query.Start {
		return errValidationS("ListPoints", "end date should be equal or bigger than start date")
	}

	if len(query.Merge) > 0 {

		for _, ks := range query.Merge {
			if len(ks.Keys) < 2 {
				return errValidationS(
					"ListPoints",
					"At least two different timeseries are required to create a merged one",
				)
			}
			t := 0
			for _, k := range ks.Keys {
				if k.TSid == "" {
					return errValidationS("ListPoints", "tsid cannot be empty")
				}
				if k.TSid[:1] == "T" {
					t++
				}
			}
			if t > 0 {
				if len(ks.Keys) != t {
					return errValidationS(
						"ListPoints",
						"Cannot merge number series with text series. Please group series by type",
					)
				}
			}
		}
	}

	if len(query.Merge) == 0 && len(query.Keys) == 0 && len(query.Text) == 0 {
		return errValidationS(
			"ListPoints",
			"No IDs found. At least one key or one text or one merge needs to be present",
		)
	}

	if query.Downsample.Enabled {
		if query.Downsample.Options.Downsample != "avg" &&
			query.Downsample.Options.Downsample != "max" &&
			query.Downsample.Options.Downsample != "min" &&
			query.Downsample.Options.Downsample != "sum" &&
			query.Downsample.Options.Downsample != "pnt" {

			return errValidationS(
				"ListPoints",
				"valid approximation values are 'avg' 'sum' 'max' 'min' 'ptn'",
			)
		}
	}

	for _, k := range query.Keys {
		if k.TSid[:1] == "T" {
			return errValidationS(
				"ListPoints",
				"key array does no support text keys, text keys should be in the text array",
			)
		}
	}

	if query.TextSearch != "" {
		re, err := regexp.Compile(query.TextSearch)
		if err != nil {
			return errValidation(
				"ListPoints",
				"invalid regular expression at textSearch",
				err,
			)
		}
		query.re = re
	}

	return nil
}

type Downsample struct {
	Enabled     bool      `json:"enabled"`
	PointLimit  bool      `json:"pointLimit"`
	TotalPoints int       `json:"totalPoints"`
	Options     DSoptions `json:"options"`
}

type DSoptions struct {
	Downsample string `json:"approximation"`
	Unit       string `json:"unit"`
	Value      int    `json:"value"`
	Fill       string
}

type Key struct {
	TSid string `json:"tsid"`
}

type Merge struct {
	Option string `json:"option"`
	Keys   []Key  `json:"keys"`
}

type Series struct {
	Text   interface{} `json:"text,omitempty"`
	Trend  interface{} `json:"trend,omitempty"`
	Points interface{} `json:"points,omitempty"`
}

type DataOperations struct {
	Downsample  Downsample
	Merge       string
	Rate        RateOperation
	Order       []string
	FilterValue FilterValueOperation
}

type RateOperation struct {
	Enabled bool
	Options TSDBrateOptions
}

type TSDBrateOptions struct {
	Counter    bool   `json:"counter"`
	CounterMax *int64 `json:"counterMax,omitempty"`
	ResetValue int64  `json:"resetValue,omitempty"`
}

type FilterValueOperation struct {
	Enabled  bool
	BoolOper string
	Value    float64
}

type SeriesType struct {
	Count int         `json:"count"`
	Total int         `json:"total"`
	Type  string      `json:"type,omitempty"`
	Ts    interface{} `json:"ts"`
}

type TSmeta struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tags"`
}

func (tsm TSmeta) Validate() gobol.Error {
	return nil
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}

type TS struct {
	index int
	Count int
	Total int
	Data  gorilla.Pnts
	gerr  gobol.Error
}

type TST struct {
	index int
	Count int
	Total int
	Data  depot.TextPnts
	gerr  gobol.Error
}

type EsResponseTag struct {
	Took     int                  `json:"took"`
	TimedOut bool                 `json:"timed_out"`
	Shards   EsRespShards         `json:"_shards"`
	Hits     EsRespHitsWrapperTag `json:"hits"`
}

type EsRespShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
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

type TagKey struct {
	Key string `json:"key"`
}

type EsResponseMetric struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   EsRespShards            `json:"_shards"`
	Hits     EsRespHitsWrapperMetric `json:"hits"`
}

type EsRespHitsWrapperMetric struct {
	Total    int                `json:"total"`
	MaxScore float32            `json:"max_score"`
	Hits     []EsRespHitsMetric `json:"hits"`
}

type EsRespHitsMetric struct {
	Index   string     `json:"_index"`
	Type    string     `json:"_type"`
	Id      string     `json:"_id"`
	Version int        `json:"_version,omitempty"`
	Found   bool       `json:"found,omitempty"`
	Score   float32    `json:"_score"`
	Source  MetricName `json:"_source"`
}

type MetricName struct {
	Name string `json:"name"`
}

type EsResponseTagKey struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   EsRespShards            `json:"_shards"`
	Hits     EsRespHitsWrapperTagKey `json:"hits"`
}

type EsRespHitsWrapperTagKey struct {
	Total    int                `json:"total"`
	MaxScore float32            `json:"max_score"`
	Hits     []EsRespHitsTagKey `json:"hits"`
}

type EsRespHitsTagKey struct {
	Index   string  `json:"_index"`
	Type    string  `json:"_type"`
	Id      string  `json:"_id"`
	Version int     `json:"_version,omitempty"`
	Found   bool    `json:"found,omitempty"`
	Score   float32 `json:"_score"`
	Source  TagKey  `json:"_source"`
}

type EsResponseTagValue struct {
	Took     int                       `json:"took"`
	TimedOut bool                      `json:"timed_out"`
	Shards   EsRespShards              `json:"_shards"`
	Hits     EsRespHitsWrapperTagValue `json:"hits"`
}

type EsRespHitsWrapperTagValue struct {
	Total    int                  `json:"total"`
	MaxScore float32              `json:"max_score"`
	Hits     []EsRespHitsTagValue `json:"hits"`
}

type EsRespHitsTagValue struct {
	Index   string   `json:"_index"`
	Type    string   `json:"_type"`
	Id      string   `json:"_id"`
	Version int      `json:"_version,omitempty"`
	Found   bool     `json:"found,omitempty"`
	Score   float32  `json:"_score"`
	Source  TagValue `json:"_source"`
}

type TagValue struct {
	Value string `json:"value"`
}

type EsResponseMeta struct {
	Took     int                   `json:"took"`
	TimedOut bool                  `json:"timed_out"`
	Shards   EsRespShards          `json:"_shards"`
	Hits     EsRespHitsWrapperMeta `json:"hits"`
}

type EsRespHitsWrapperMeta struct {
	Total    int              `json:"total"`
	MaxScore float32          `json:"max_score"`
	Hits     []EsRespHitsMeta `json:"hits"`
}

type EsRespHitsMeta struct {
	Index   string   `json:"_index"`
	Type    string   `json:"_type"`
	Id      string   `json:"_id"`
	Version int      `json:"_version,omitempty"`
	Found   bool     `json:"found,omitempty"`
	Score   float32  `json:"_score"`
	Source  MetaInfo `json:"_source"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
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

type TsMetaInfo struct {
	TsId   string            `json:"id"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type EsNestedQuery struct {
	Nested EsNested `json:"nested"`
}

type EsNested struct {
	Path      string      `json:"path"`
	ScoreMode string      `json:"score_mode,omitempty"`
	Query     BoolWrapper `json:"filter"`
}

type Term struct {
	Term map[string]string `json:"term"`
}

type TSDBfilter struct {
	Ftype   string `json:"type"`
	Tagk    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

type TSDBobj struct {
	Tsuid  string            `json:"tsuid"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

type TSDBlookup struct {
	Type         string    `json:"type"`
	Metric       string    `json:"metric"`
	Tags         []Tag     `json:"tags"`
	Limit        int       `json:"limit"`
	Time         int       `json:"time"`
	Results      []TSDBobj `json:"results"`
	StartIndex   int       `json:"startIndex"`
	TotalResults int       `json:"totalResults"`
}

type TSDBresponses []TSDBresponse

func (r TSDBresponses) Len() int {
	return len(r)
}

func (r TSDBresponses) Less(i, j int) bool {

	if (r[i].Query != nil && r[i].Query.Index != nil) && (r[j].Query != nil && r[j].Query.Index != nil) {
		if *r[i].Query.Index != *r[j].Query.Index {
			return *r[i].Query.Index < *r[j].Query.Index
		}
	}

	if r[i].Metric != r[j].Metric {
		return r[i].Metric < r[j].Metric
	}

	keys := []string{}

	for k := range r[i].Tags {
		keys = append(keys, k)
	}

	for kj := range r[j].Tags {

		add := true

		for _, k := range keys {
			if k == kj {
				add = false
			}
		}

		if add {
			keys = append(keys, kj)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		if vi, ok := r[i].Tags[k]; ok {
			if vj, ok := r[j].Tags[k]; ok {
				if vi == vj {
					continue
				}
				return vi < vj
			}
			return true
		}
		return false
	}

	sort.Strings(r[i].AggregatedTags)
	sort.Strings(r[j].AggregatedTags)

	for _, ati := range r[i].AggregatedTags {
		for _, atj := range r[j].AggregatedTags {
			if ati == atj {
				break
			}
			return ati < atj
		}
	}

	return false
}

func (r TSDBresponses) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type TSDBresponse struct {
	Metric         string             `json:"metric"`
	Tags           map[string]string  `json:"tags"`
	AggregatedTags []string           `json:"aggregateTags"`
	Query          *structs.TSDBquery `json:"query,omitempty"`
	TSUIDs         []string           `json:"tsuids"`
	Dps            *TSMarshaler       `json:"dps"`
}

type ExpParse struct {
	Expression string `json:"expression"`
	Expand     bool   `json:"expand"`
	Keyspace   string `json:"keyspace"`
}

func (expp ExpParse) Validate() gobol.Error {
	return nil
}

type ExpQuery struct {
	Expression string `json:"expression"`
}

func (eq ExpQuery) Validate() gobol.Error {
	return nil
}
