package meta

import (
	"time"

	"github.com/uol/gobol"
)

func (meta *elasticMeta) ListTags(keyspace, dtype, tagkey string, size, from int64) ([]string, int, gobol.Error) {
	var query elasticQueryWrapper
	if tagkey != "" {
		tagTerm := elasticRegexp{
			Regexp: map[string]string{
				"key": tagkey,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, tagTerm)
	}

	query.From = from
	query.Size = 50
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseTag
	start := time.Now()
	_, err := meta.esearch.Query(keyspace, dtype, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, dtype, "post")
		return nil, 0, errPersist("ListESTags", err)
	}
	statsIndex(meta.stats, keyspace, dtype, "post", time.Since(start))

	total := response.Hits.Total

	var tags []string
	for _, docs := range response.Hits.Hits {
		tags = append(tags, docs.ID)
	}
	return tags, total, nil
}

func (meta *elasticMeta) ListMetrics(keyspace, dtype, metric string, size, from int64) ([]string, int, gobol.Error) {
	var query elasticQueryWrapper
	if metric != "" {
		metricTerm := elasticRegexp{
			Regexp: map[string]string{
				"metric": metric,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, metricTerm)
	}

	query.From = from
	query.Size = 50
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseMetric
	start := time.Now()
	_, err := meta.esearch.Query(keyspace, dtype, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, dtype, "post")
		return nil, 0, errPersist("ListESMetrics", err)
	}
	statsIndex(meta.stats, keyspace, dtype, "post", time.Since(start))
	total := response.Hits.Total

	var metrics []string
	for _, docs := range response.Hits.Hits {
		metrics = append(metrics, docs.ID)
	}
	return metrics, total, nil
}

func (meta *elasticMeta) ListTagKey(keyspace, tagKname string, size, from int64) ([]string, int, gobol.Error) {
	dtype := "tagk"
	var query elasticQueryWrapper
	if tagKname != "" {
		tagKterm := elasticRegexp{
			Regexp: map[string]string{
				"key": tagKname,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, tagKterm)
	}

	query.From = from
	query.Size = 50
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseTagKey

	start := time.Now()
	_, err := meta.esearch.Query(keyspace, dtype, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, dtype, "post")
		return nil, 0, errPersist("ListESTagKey", err)
	}
	statsIndex(meta.stats, keyspace, dtype, "post", time.Since(start))

	total := response.Hits.Total
	var tagKs []string
	for _, docs := range response.Hits.Hits {
		tagKs = append(tagKs, docs.ID)
	}
	return tagKs, total, nil
}

func (meta *elasticMeta) ListTagValue(keyspace, tagVname string, size, from int64) ([]string, int, gobol.Error) {
	esType := "tagv"
	var query elasticQueryWrapper
	if tagVname != "" {
		tagVterm := elasticRegexp{
			Regexp: map[string]string{
				"value": tagVname,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, tagVterm)
	}

	query.From = from
	query.Size = 50
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseTagValue

	start := time.Now()
	_, err := meta.esearch.Query(keyspace, esType, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, esType, "post")
		return nil, 0, errPersist("ListESTagValue", err)
	}
	statsIndex(meta.stats, keyspace, esType, "post", time.Since(start))

	total := response.Hits.Total
	var tagVs []string
	for _, docs := range response.Hits.Hits {
		tagVs = append(tagVs, docs.ID)
	}
	return tagVs, total, nil
}

func (meta *elasticMeta) ListMeta(
	keyspace, esType, metric string, tags map[string]string,
	onlyids bool, size, from int64,
) ([]TSInfo, int, gobol.Error) {
	var query elasticQueryWrapper
	if metric != "" {
		metricTerm := elasticRegexp{
			Regexp: map[string]string{
				"metric": metric,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, metricTerm)
	}

	for k, v := range tags {
		var esQueryNest elasticNestedQuery
		esQueryNest.Nested.Path = "tagsNested"
		if k != "" || v != "" {
			if k == "" {
				k = ".*"
			}
			if v == "" {
				v = ".*"
			}
			tagKTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsNested.tagKey": k,
				},
			}
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)
			tagVTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsNested.tagValue": v,
				},
			}
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, esQueryNest)
	}

	query.From = from
	query.Size = 50
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
	var tsMetaInfos []TSInfo
	for _, docs := range response.Hits.Hits {
		var tsmi TSInfo
		if !onlyids {
			tsmi = TSInfo{
				Metric: docs.Source.Metric,
				TSID:   docs.Source.ID,
				Tags:   map[string]string{},
			}

			for _, tag := range docs.Source.Tags {
				tsmi.Tags[tag.Key] = tag.Value
			}
		} else {
			tsmi = TSInfo{
				TSID: docs.Source.ID,
			}
		}

		tsMetaInfos = append(tsMetaInfos, tsmi)
	}
	return tsMetaInfos, total, nil
}

func (meta *elasticMeta) ListErrorTags(
	keyspace, esType, metric string,
	tags []Tag, size, from int64,
) ([]string, int, gobol.Error) {
	var query elasticQueryWrapper
	if metric != "" {
		metricTerm := elasticRegexp{
			Regexp: map[string]string{
				"metric": metric,
			},
		}
		query.Query.Bool.Must = append(query.Query.Bool.Must, metricTerm)
	}

	for _, tag := range tags {
		if tag.Key != "" {
			tagKeyTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsError.tagKey": tag.Key,
				},
			}
			query.Query.Bool.Must = append(query.Query.Bool.Must, tagKeyTerm)
		}

		if tag.Value != "" {
			tagValueTerm := elasticRegexp{
				Regexp: map[string]string{
					"tagsError.tagValue": tag.Value,
				},
			}
			query.Query.Bool.Must = append(query.Query.Bool.Must, tagValueTerm)
		}
	}

	query.From = from
	query.Size = 50
	if size != 0 {
		query.Size = size
	}

	var response elasticResponseTag
	start := time.Now()
	_, err := meta.esearch.Query(keyspace, esType, query, &response)
	if err != nil {
		statsIndexError(meta.stats, keyspace, esType, "POST")
		return nil, 0, errPersist("ListESErrorTags", err)
	}
	statsIndex(meta.stats, keyspace, esType, "POST", time.Since(start))

	total := response.Hits.Total
	var keys []string
	for _, docs := range response.Hits.Hits {
		keys = append(keys, docs.ID)
	}

	return keys, total, nil
}
