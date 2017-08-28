package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/tsstats"
)

type esIndex struct {
	endpoint string
	index    string
	esType   string

	client *rubber.Elastic
	stats  *tsstats.StatsTS
}

func createESIndex(host, index, itype string, logger *zap.Logger) (*esIndex, error) {
	client, err := rubber.New(logger, rubber.Settings{
		Seed:    fmt.Sprintf("%s:9200", host),
		Type:    rubber.ConfigWeightedBackend,
		Timeout: time.Minute,
	})

	if err != nil {
		return nil, err
	}
	newIndex := &esIndex{
		endpoint: fmt.Sprintf("%s:9200", host),
		index:    index,
		esType:   itype,

		client: client,
	}
	return newIndex, nil
}

// CreateElastic creates an elastic backend
func CreateElastic(host, index, itype string, logger *zap.Logger) (Backend, error) {
	return createESIndex(host, index, itype, logger)
}

func (i *esIndex) Add(Metric, []KVPair, ID) error { return nil }

func (i *esIndex) CheckResult(id ID, m Metric, ps []KVPair, fs []Filter) bool {
	var (
		url = fmt.Sprintf("http://%s/%s/%s/%d", i.endpoint, i.index, i.esType, id)
	)

	var content struct {
		ID   string `json:"_id"`
		Data struct {
			ID     string `json:"id"`
			Metric string `json:"metric"`
			Tags   []struct {
				Key   string `json:"tagKey"`
				Value string `json:"tagValue"`
			} `json:"tagsNested"`
		} `json:"_source"`
	}

	response, err := http.Get(url)
	if err != nil {
		return false
	}
	defer response.Body.Close()

	if err := json.NewDecoder(response.Body).Decode(&content); err != nil {
		return false
	}

	if content.Data.Metric != m.String() {
		return false
	}
	for _, pair := range ps {
		var found bool
		for _, tag := range content.Data.Tags {
			if tag.Key == pair.Key && tag.Value == tag.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	for _, filter := range fs {
		var found bool
		re, err := regexp.Compile(filter.Expression)
		if err != nil {
			return false
		}
		for _, tag := range content.Data.Tags {
			if tag.Key == filter.Key && re.MatchString(tag.Value) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (i *esIndex) ListMetric(regexp string) ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (i *esIndex) ListTagKeys(regexp string) ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (i *esIndex) ListTagValues(string, string) ([]string, error) {
	return nil, fmt.Errorf("Not implemented")
}

type queryMatch struct {
	Match map[string]string `json:"match"`
}

func (i *esIndex) Query(m Metric, ps []KVPair, fs []Filter) ResultSet {
	var (
		url         = fmt.Sprintf("http://%s/%s/%s/_search", i.endpoint, i.index, i.esType)
		contentType = "application/json"
		body        = bytes.NewBuffer(nil)

		answ       = makeResultSet()
		scrollSize = 5000
	)

	var content struct {
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Count int `json:"total"`
			Hits  []struct {
				ID   string `json:"_id"`
				Data struct {
					ID     string `json:"id"`
					Metric string `json:"metric"`
					Tags   []struct {
						Key   string `json:"tagKey"`
						Value string `json:"tagValue"`
					} `json:"tagsNested"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	var query queryWrapper
	query.Size = 500
	query.Query.Bool.Must = make([]interface{}, 1+len(ps)+len(fs))

	query.Query.Bool.Must[0] = esTerm{
		Term: map[string]string{
			"metric": m.String(),
		},
	}

	for i, pair := range ps {
		var nested esNestedQuery
		nested.Nested.Path = "tagsNested"
		nested.Nested.Query.Bool.Should = []interface{}{
			esTerm{
				Term: map[string]string{
					"tagsNested.tagKey":   pair.Key,
					"tagsNested.tagValue": pair.Value,
				},
			},
		}
		query.Query.Bool.Must[i+1] = nested
	}

	for i, pair := range fs {
		var nested esNestedQuery
		nested.Nested.Path = "tagsNested"
		nested.Nested.Query.Bool.Must = []interface{}{
			queryMatch{
				Match: map[string]string{
					"tagsNested.tagKey": pair.Key,
				},
			},
			esRegexp{
				Regexp: map[string]string{
					"tagsNested.tagValue": pair.Expression,
				},
			},
		}
		query.Query.Bool.Must[i+len(ps)+1] = nested
	}

	if err := json.NewEncoder(body).Encode(query); err != nil {
		return emptySet
	}

	for {
		resp, err := http.Post(url, contentType, body)
		if err != nil {
			return emptySet
		}

		if err = json.NewDecoder(resp.Body).Decode(&content); err != nil {
			return emptySet
		}

		for _, hit := range content.Hits.Hits {
			var (
				p = make([]KVPair, len(hit.Data.Tags))

				id  ID
				val uint64
			)
			val, err = strconv.ParseUint(hit.Data.ID, 10, 64)
			if err != nil {
				return emptySet
			}
			id = ID(val)

			for i, tag := range hit.Data.Tags {
				p[i] = KVPair{
					Key:   tag.Key,
					Value: tag.Value,
				}
			}
			answ.Add(id)
		}
		resp.Body.Close()

		if len(content.Hits.Hits) < scrollSize {
			return answ
		}

		url = fmt.Sprintf("http://%s/_search/scroll", i.endpoint)
		body = bytes.NewBufferString(
			fmt.Sprintf(`{"scroll": "1m", "scroll_id": "%s"}`, content.ScrollID),
		)
	}
}

func (i *esIndex) Exists(id ID) (bool, error) {
	start := time.Now()
	status, err := i.client.GetHead(i.index, i.esType, id.String())
	if err != nil {
		statsIndexError(i.stats, i.index, i.esType, "head")
		return false, err
	}

	statsIndex(i.stats, i.index, i.esType, "head", time.Since(start))
	return status == http.StatusOK, nil
}

func (i *esIndex) Store(io.Writer) error { return nil }
func (i *esIndex) Load(io.Reader) error  { return nil }
