package meta

import (
	"strings"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/structs"
)

// TSDBData is data in TSDB format
type TSDBData struct {
	Tsuid  string            `json:"tsuid"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

func (meta *elasticMeta) MetaOpenTSDB(
	keyspace, id, metric string, tags map[string][]string,
	size, from int64,
) ([]TSDBData, int, gobol.Error) {
	esType := elasticMetaType
	var query elasticQueryWrapper
	if metric != "" && metric != "*" {
		metricTerm := elasticTerm{
			Term: map[string]string{
				"metric": metric,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, metricTerm)
	}

	for k, vs := range tags {
		var esQueryNest elasticNestedQuery
		esQueryNest.Nested.Path = elasticNestedPath
		for _, v := range vs {
			tagKTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsNested.tagKey": k,
				},
			}

			if v == "*" {
				v = ".*"
			}
			tagVTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsNested.tagValue": v,
				},
			}

			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)
		}

		query.Query.Bool.Must = append(query.Query.Bool.Must, esQueryNest)
	}

	query.From = from
	query.Size = 10000
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseMeta
	start := time.Now()
	_, err := meta.esearch.Query(keyspace, esType, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, esType, "post")
		return nil, 0, errPersist("ListESMeta", err)
	}
	statsIndex(meta.stats, keyspace, esType, "post", time.Since(start))

	total := response.Hits.Total
	var tsds []TSDBData
	for _, docs := range response.Hits.Hits {
		mapTags := map[string]string{}
		for _, tag := range docs.Source.Tags {
			mapTags[tag.Key] = tag.Value
		}

		tsd := TSDBData{
			Tsuid:  docs.Source.ID,
			Metric: docs.Source.Metric,
			Tags:   mapTags,
		}
		tsds = append(tsds, tsd)
	}
	return tsds, total, nil
}

func (meta *elasticMeta) MetaFilterOpenTSDB(
	keyspace, id, metric string,
	filters []structs.TSDBfilter, size int64,
) ([]TSDBData, int, gobol.Error) {
	esType := elasticMetaType
	query := elasticQueryWrapper{
		Size: size,
	}

	if metric != "" && metric != "*" {
		metricTerm := elasticTerm{
			Term: map[string]string{
				"metric": metric,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, metricTerm)
	}

	for _, filter := range filters {
		var esQueryNest elasticNestedQuery
		esQueryNest.Nested.Path = "tagsNested"
		tk := filter.Tagk
		tk = strings.Replace(tk, ".", "\\.", -1)
		tk = strings.Replace(tk, "&", "\\&", -1)
		tk = strings.Replace(tk, "#", "\\#", -1)

		tagKTerm := elasticRegexp{
			Regexp: map[string]string{
				"tagsNested.tagKey": tk,
			},
		}

		v := filter.Filter
		if filter.Ftype != "regexp" {
			v = strings.Replace(v, ".", "\\.", -1)
			v = strings.Replace(v, "&", "\\&", -1)
			v = strings.Replace(v, "#", "\\#", -1)
		}

		if filter.Ftype == "wildcard" {
			v = strings.Replace(v, "*", ".*", -1)
		}

		tagVTerm := elasticRegexp{
			Regexp: map[string]string{
				"tagsNested.tagValue": v,
			},
		}

		esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)
		if filter.Ftype == "not_literal_or" {
			esQueryNest.Nested.Query.Bool.MustNot = append(esQueryNest.Nested.Query.Bool.MustNot, tagVTerm)
		} else {
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, esQueryNest)
	}

	var response elasticResponseMeta
	start := time.Now()
	_, err := meta.esearch.Query(keyspace, esType, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, esType, "post")
		return nil, 0, errPersist("ListESMeta", err)
	}
	statsIndex(meta.stats, keyspace, esType, "post", time.Since(start))

	total := response.Hits.Total
	var tsds []TSDBData
	for _, docs := range response.Hits.Hits {
		mapTags := map[string]string{}
		for _, tag := range docs.Source.Tags {
			mapTags[tag.Key] = tag.Value
		}
		tsd := TSDBData{
			Tsuid:  docs.Source.ID,
			Metric: docs.Source.Metric,
			Tags:   mapTags,
		}
		tsds = append(tsds, tsd)
	}
	return tsds, total, nil
}
