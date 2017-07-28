package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type tsError struct {
	ID      string
	Error   string
	Message string
	Date    time.Time
}

type tsErrorV2 struct {
	Metric string
	Tags   []tsTagV2 `json:"tagsError"`
}

type tsTagV2 struct {
	TagKey   string
	TagValue string
}

type tsErrorTags struct {
	TotalRecords int
	Payload      []string
}

var delay time.Duration = 2 * time.Second

func postAndCheckError(test, path string, data interface{}, status int, t *testing.T) {

	body, _ := json.Marshal(data)
	code, content, err := mycenaeTools.HTTP.POST(path, body)
	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae), test)
	assert.Equal(t, status, code, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae), test)

	if status == 200 {
		var records tsErrorTags

		err = json.Unmarshal(content, &records)
		assert.Equal(t, 1, records.TotalRecords, fmt.Sprintf("The request to get error list on keyspace %v did not return the array expected", ksMycenae), test)
		assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error list on keyspace %v", ksMycenae), test)
	}
}

func getErrorPayload(metric string) (string, string, string) {

	random := rand.Int()
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)

	return fmt.Sprintf(
			`{"metric": "%s", "tags": {"ksid": "%s", "%s": "%s"}, "timestamp": %d}`,
			metric, ksMycenae, tagKey, tagValue, time.Now().Unix(),
		),
		tagKey,
		tagValue
}

func getErrorPayload2Tags(metric string) (string, string, string, string, string) {

	random := rand.Int()
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)

	return fmt.Sprintf(
			`{"metric": "%s", "tags": {"ksid": "%s", "%s1": "%s1", "%s2": "%s2"}, "timestamp": %d}`,
			metric, ksMycenae, tagKey, tagValue, tagKey, tagValue, time.Now().Unix(),
		),
		fmt.Sprint(tagKey, "1"),
		fmt.Sprint(tagValue, "1"),
		fmt.Sprint(tagKey, "2"),
		fmt.Sprint(tagValue, "1")
}

func TestGetTimeseriesV2Error(t *testing.T) {
	t.Parallel()
	metric := "TestGetTimeseriesV2Error"

	payload, tk, tv := getErrorPayload(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tk: tv})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)
	code, content, err := mycenaeTools.HTTP.GET(path)
	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tsError{}
	err = json.Unmarshal(content, &data)
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))

	assert.Equal(t, 1, len(data), fmt.Sprintf("The result is different than expected"))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, "Wrong Format: Field \"value\" is required. NO information will be saved", data[0].Error)
	assert.Equal(t, payload, data[0].Message)
}

func TestGetTimeseriesV2MultipleError(t *testing.T) {
	t.Parallel()

	metric := "metric"
	tagKey := "testTagKey"
	tagValue := "testTagValue?"
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)
	timestamp2 := time.Now().Unix()

	payload = fmt.Sprintf(
		`{"value": 1.0, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, content, err := mycenaeTools.HTTP.GET(path)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tsError{}

	err = json.Unmarshal(content, &data)
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))

	message2 := fmt.Sprintf(`{"value": 1.0, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2)

	assert.Condition(t, func() bool { return len(data) == 1 }, "The result is different than expected")
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Contains(t, data[0].Error, "Wrong Format: Tag value (testTagValue?) is not well formed. NO information will be saved")
	assert.Equal(t, message2, data[0].Message)

}

func TestGetTimeseriesV2SameError(t *testing.T) {
	t.Parallel()

	metric := "metric"
	tagKey := "testTagKey"
	tagValue := "testTagValue"
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)
	timestamp2 := time.Now().Unix()

	payload = fmt.Sprintf(
		`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
	bucket := fmt.Sprintf("%v%v", hashID, ksMycenae)

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, content, err := mycenaeTools.HTTP.GET(path)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, 200, code, fmt.Sprintf("The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae))

	data := []tsError{}

	err = json.Unmarshal(content, &data)

	message := fmt.Sprintf(`{"metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp2)

	assert.Equal(t, 1, len(data), fmt.Sprintf("The result is different than expected"))
	assert.NoError(t, err, fmt.Sprintf("It was not possible to unmarshal the response for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, bucket, data[0].ID, "ts_", fmt.Sprintf("We received a weird keyspace name for request to get error %v on keyspace %v", hashID, ksMycenae))
	assert.Equal(t, "Wrong Format: Field \"value\" is required. NO information will be saved", data[0].Error)
	assert.Equal(t, message, data[0].Message)

}

func TestGetTimeseriesV2Errors(t *testing.T) {

	cases := map[string]struct {
		hashID string
		ksid   string
		status int
	}{
		"NoContent": {
			"notExists",
			ksMycenae,
			204,
		},
		"KeyspaceNotExists": {
			"notExists",
			"notExists",
			404,
		},
		"EmptyKeyspace": {
			"notExists",
			"",
			404,
		},
		"EmptyErrorId": {
			"",
			ksMycenae,
			404,
		},
	}

	for test, data := range cases {

		path := fmt.Sprintf("keyspaces/%s/errors/%s", data.ksid, data.hashID)
		code, _, err := mycenaeTools.HTTP.GET(path)

		assert.NoError(t, err, "There was an error with request to get error %v on keyspace %v and test %s", data.hashID, data.ksid, test)
		assert.Equal(t, data.status, code, "The request to get error %v on keyspace %v did not return the expected http code. Test: %s", data.hashID, data.ksid, test)
	}
}

func TestGetTimeseriesV2ErrorPost(t *testing.T) {
	t.Parallel()

	random := rand.Int()
	metric := ""
	value := 5.0
	tagKey := fmt.Sprint("testTagKey-", random)
	tagValue := fmt.Sprint("testTagValue-", random)
	timestamp := time.Now().Unix()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})

	path := fmt.Sprintf("keyspaces/%s/errors/%s", ksMycenae, hashID)

	code, _, err := mycenaeTools.HTTP.POST(path, nil)

	assert.NoError(t, err, "There was an error with request to get error %v on keyspace %v", hashID, ksMycenae)
	assert.Equal(t, 405, code, "The request to get error %v on keyspace %v did not return the expected http code", hashID, ksMycenae)

}

func TestListTimeseriesV2ErrorIdsMultipleTags(t *testing.T) {
	t.Parallel()

	metric := "TestListTimeseriesV2ErrorIdsMultipleTags"

	payload, tk1, tv1, tk2, tv2 := getErrorPayload2Tags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		metric,
		[]tsTagV2{{tk1, tv1}, {tk2, tv2}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsMetric(t *testing.T) {
	t.Parallel()
	type tsErrorV2 struct {
		Metric string
	}

	metric := fmt.Sprint("TestListTimeseriesV2ErrorIdsMetric-", time.Now().Unix())

	payload, _, _, _, _ := getErrorPayload2Tags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		metric,
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsTagKey(t *testing.T) {
	t.Parallel()

	type tsTagV2 struct {
		TagKey string
	}

	type tsErrorV2 struct {
		Tags []tsTagV2 `json:"tagsError"`
	}

	payload, tk1, _, _, _ := getErrorPayload2Tags("TestListTimeseriesV2ErrorIdsTagKey")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		[]tsTagV2{{tk1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsTagValue(t *testing.T) {
	t.Parallel()

	type tsTagV2 struct {
		TagValue string
	}

	type tsErrorV2 struct {
		Tags []tsTagV2 `json:"tagsError"`
	}

	payload, _, tv1, _, _ := getErrorPayload2Tags("TestListTimeseriesV2ErrorIdsTagValue")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		[]tsTagV2{{tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsTagKeyAndTagValue(t *testing.T) {
	t.Parallel()

	type tsTagV2 struct {
		TagKey   string
		TagValue string
	}

	type tsErrorV2 struct {
		Tags []tsTagV2 `json:"tagsError"`
	}

	payload, tk1, tv1, _, _ := getErrorPayload2Tags("TestListTimeseriesV2ErrorIdsTagKeyAndTagValue")
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		[]tsTagV2{{tk1, tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsTagKeyAndMetric(t *testing.T) {
	t.Parallel()

	type tsTagV2 struct {
		TagKey string
	}

	type tsErrorV2 struct {
		Metric string
		Tags   []tsTagV2 `json:"tagsError"`
	}

	metric := "TestListTimeseriesV2ErrorIdsTagKeyAndMetric"

	payload, tk1, _, _, _ := getErrorPayload2Tags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		metric,
		[]tsTagV2{{tk1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsTagValueAndMetric(t *testing.T) {
	t.Parallel()

	type tsTagV2 struct {
		TagValue string
	}

	type tsErrorV2 struct {
		Metric string
		Tags   []tsTagV2 `json:"tagsError"`
	}

	metric := "TestListTimeseriesV2ErrorIdsTagValueAndMetric"

	payload, _, tv1, _, _ := getErrorPayload2Tags(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		metric,
		[]tsTagV2{{tv1}},
	}

	postAndCheckError("", path, data, 200, t)
}

func TestListTimeseriesV2ErrorIdsNoResults(t *testing.T) {
	t.Parallel()

	_, tk, tv := getErrorPayload("")

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		"metric.*",
		[]tsTagV2{{tk, tv}},
	}

	postAndCheckError("", path, data, 204, t)
}

func TestListTimeseriesV2ErrorIds(t *testing.T) {
	t.Parallel()
	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	metric := "metricTest"

	payload, tk, tv := getErrorPayload(metric)
	mycenaeTools.UDP.SendString(payload)
	time.Sleep(delay)

	cases := map[string]struct {
		body   tsErrorV2
		status int
	}{
		"MetricRegexp": {
			tsErrorV2{
				"metric.*",
				[]tsTagV2{{tk, tv}},
			}, 200,
		},
		"WrongTagValue": {
			tsErrorV2{
				metric,
				[]tsTagV2{{tk, fmt.Sprint(tv, "1")}},
			}, 204,
		},
		"WrongTagKey": {
			tsErrorV2{
				metric,
				[]tsTagV2{{fmt.Sprint(tk, "1"), tv}},
			}, 204,
		},
		"WrongMetric": {
			tsErrorV2{
				fmt.Sprint(metric, "1"),
				[]tsTagV2{{tk, tv}},
			}, 204,
		},
	}

	for test, data := range cases {

		postAndCheckError(test, path, data.body, data.status, t)
	}
}

func TestGetTimeseriesV2MultipleErrorIdsKeyspaceNotExists(t *testing.T) {

	data := tsErrorV2{
		"metric.*",
		[]tsTagV2{{}},
	}

	postAndCheckError("", "keyspaces/notExists/errortags", data, 404, t)
}

func TestGetTimeseriesV2MultipleErrorIdsParamsSizeAndFrom(t *testing.T) {

	cases := map[string]string{
		"SizeString":   "size=a",
		"NegativeSize": "size=-1",
		"SizeZero":     "size=0",
		"NegativeFrom": "from=-1",
	}

	for test, param := range cases {

		path := fmt.Sprintf("keyspaces/%s/errortags?%s", ksMycenae, param)
		data := tsErrorV2{
			"metric.*",
			[]tsTagV2{{}},
		}

		postAndCheckError(test, path, data, 400, t)
	}
}

func TestGetTimeseriesV2MalformedPayload(t *testing.T) {

	type tsErrorV2 struct {
		Metric int
		Tags   []tsTagV2 `json:"tagsError"`
	}

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		1,
		[]tsTagV2{{"tagKey", "tagValue"}},
	}

	postAndCheckError("", path, data, 400, t)
}

func TestGetTimeseriesV2FieldNotExists(t *testing.T) {

	type tsErrorV2_1 struct {
		Test string `json:"metric1"`
	}

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2_1{
		"test",
	}

	body, _ := json.Marshal(data)

	code, _, err := mycenaeTools.HTTP.POST(path, body)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae))
	assert.True(t, code == 200 || code == 204, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae))

}

func TestGetTimeseriesV2MethodNotAllowed(t *testing.T) {

	path := fmt.Sprintf("keyspaces/%s/errortags", ksMycenae)
	data := tsErrorV2{
		"metric",
		[]tsTagV2{{"tagKey", "tagValue"}},
	}

	body, _ := json.Marshal(data)

	code, _, err := mycenaeTools.HTTP.PUT(path, body)

	assert.NoError(t, err, fmt.Sprintf("There was an error with request to get error list on keyspace %v", ksMycenae))
	assert.Equal(t, 405, code, fmt.Sprintf("The request to get error list on keyspace %v did not return the expected http code", ksMycenae))

}
