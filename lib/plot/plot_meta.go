package plot

import (
	"github.com/uol/gobol"
)

func (plot Plot) ListTags(keyspace, esType, tagKey string, size, from int64) ([]string, int, gobol.Error) {

	var esQuery QueryWrapper

	if tagKey != "" {

		tagTerm := EsRegexp{
			Regexp: map[string]string{
				"key": tagKey,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, tagTerm)
	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseTag

	gerr := plot.persist.ListESTags(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tags []string

	for _, docs := range esResp.Hits.Hits {

		tag := docs.Id

		tags = append(tags, tag)

	}

	return tags, total, gerr
}

func (plot Plot) ListMetrics(keyspace, esType, metricName string, size, from int64) ([]string, int, gobol.Error) {

	var esQuery QueryWrapper

	if metricName != "" {

		metricTerm := EsRegexp{
			Regexp: map[string]string{
				"metric": metricName,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, metricTerm)
	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseMetric

	gerr := plot.persist.ListESMetrics(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var metrics []string

	for _, docs := range esResp.Hits.Hits {

		metric := docs.Id

		metrics = append(metrics, metric)

	}

	return metrics, total, gerr
}

func (plot Plot) ListTagKey(keyspace, tagKname string, size, from int64) ([]string, int, gobol.Error) {

	esType := "tagk"

	var esQuery QueryWrapper

	if tagKname != "" {

		tagKterm := EsRegexp{
			Regexp: map[string]string{
				"key": tagKname,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, tagKterm)
	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseTagKey

	gerr := plot.persist.ListESTagKey(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tagKs []string

	for _, docs := range esResp.Hits.Hits {

		tagk := docs.Id

		tagKs = append(tagKs, tagk)

	}

	return tagKs, total, gerr
}

func (plot Plot) ListTagValue(keyspace, tagVname string, size, from int64) ([]string, int, gobol.Error) {

	esType := "tagv"

	var esQuery QueryWrapper

	if tagVname != "" {

		tagVterm := EsRegexp{
			Regexp: map[string]string{
				"value": tagVname,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, tagVterm)
	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseTagValue

	gerr := plot.persist.ListESTagValue(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tagVs []string

	for _, docs := range esResp.Hits.Hits {

		tagv := docs.Id

		tagVs = append(tagVs, tagv)

	}

	return tagVs, total, gerr
}

func (plot Plot) ListMeta(
	keyspace,
	esType,
	metric string,
	tags map[string]string,
	onlyids bool,
	size,
	from int64,
) ([]TsMetaInfo, int, gobol.Error) {

	var esQuery QueryWrapper

	if metric != "" {

		metricTerm := EsRegexp{
			Regexp: map[string]string{
				"metric": metric,
			},
		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, metricTerm)
	}

	for k, v := range tags {

		var esQueryNest EsNestedQuery

		esQueryNest.Nested.Path = "tagsNested"

		if k != "" || v != "" {

			if k == "" {
				k = ".*"
			}

			if v == "" {
				v = ".*"
			}

			tagKTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsNested.tagKey": k,
				},
			}
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagKTerm)

			tagVTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsNested.tagValue": v,
				},
			}
			esQueryNest.Nested.Query.Bool.Must = append(esQueryNest.Nested.Query.Bool.Must, tagVTerm)

		}

		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, esQueryNest)

	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseMeta

	gerr := plot.persist.ListESMeta(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var tsMetaInfos []TsMetaInfo

	for _, docs := range esResp.Hits.Hits {

		var tsmi TsMetaInfo

		if !onlyids {

			tsmi = TsMetaInfo{
				Metric: docs.Source.Metric,
				TsId:   docs.Source.ID,
				Tags:   map[string]string{},
			}

			for _, tag := range docs.Source.Tags {
				tsmi.Tags[tag.Key] = tag.Value
			}

		} else {
			tsmi = TsMetaInfo{
				TsId: docs.Source.ID,
			}
		}

		tsMetaInfos = append(tsMetaInfos, tsmi)

	}

	return tsMetaInfos, total, gerr
}
