package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	tree "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/stretchr/testify/assert"
)

var _ Backend = &tsIndex{}

var (
	esHost      string
	integration bool
)

const (
	index      = "stats"
	scrollSize = 5000
	resultSize = 10
)

func init() {
	esHost = os.Getenv("ELASTICSEARCH")
	if esHost != "" {
		integration = true
		fmt.Fprintf(os.Stderr, "Integration: elasticsearch=%s\n", esHost)
	}
}

func fillIndexData(backend Backend, count int) {
	for i := 0; i < count; i++ {
		for j := 0; j < resultSize; j++ {
			backend.Add(
				Metric(fmt.Sprintf("testing.test%d", i)),
				[]KVPair{
					{"key-1", fmt.Sprintf("value-1-%d", i)},
					{"key-2", fmt.Sprintf("value-2-%d", i)},
					{"key-3", fmt.Sprintf("value-3-%d", i)},
				},
				ID(i+j),
			)
		}
	}
}

func TestSingleIndex(t *testing.T) {
	backend := createIndex()
	fillIndexData(backend, 1024)

	rs := backend.Query("testing.test68", []KVPair{
		{"key-1", "value-1-68"},
	}, []Filter{})
	assert.Equal(t, resultSize, rs.Len())
	assert.Equal(t, 3, len(backend.tags.Keys()))

	for _, value := range backend.tags.Values() {
		tr, ok := value.(*tree.Tree)
		assert.True(t, ok)
		assert.NotNil(t, tr)
	}

	output := bytes.NewBuffer(nil)
	backend.Store(output)
	t.Logf("Length: %d", len(output.String()))
}

func loadQAData(backend Backend) error {
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
	var body io.Reader
	url := fmt.Sprintf(
		"http://%s:9200/%s/meta/_search?scroll=1m&size=%d",
		esHost, index, scrollSize,
	)

	for {
		resp, err := http.Post(url, "application/json", body)
		if err != nil {
			return err
		}

		if err = json.NewDecoder(resp.Body).Decode(&content); err != nil {
			return err
		}

		for _, hit := range content.Hits.Hits {
			var (
				m = Metric(hit.Data.Metric)
				p = make([]KVPair, len(hit.Data.Tags))

				id  ID
				val uint64
			)
			val, err = strconv.ParseUint(hit.Data.ID, 10, 64)
			if err != nil {
				return err
			}
			id = ID(val)

			for i, tag := range hit.Data.Tags {
				p[i] = KVPair{
					Key:   tag.Key,
					Value: tag.Value,
				}
			}
			backend.Add(m, p, id)
		}
		resp.Body.Close()

		if len(content.Hits.Hits) < scrollSize {
			return nil
		}

		url = fmt.Sprintf("http://%s:9200/_search/scroll", esHost)
		body = bytes.NewBufferString(
			fmt.Sprintf(`{"scroll": "1m", "scroll_id": "%s"}`, content.ScrollID),
		)
	}
}

func TestQAData(t *testing.T) {
	if !integration {
		t.SkipNow()
	}
	backend := createIndex()
	loadQAData(backend)

	f, err := os.Create("/tmp/quality.index.json")
	if !assert.NoError(t, err) {
		return
	}
	defer f.Close()
	if !assert.NoError(t, backend.Store(f)) {
		return
	}

	restore := Create()
	f2, err := os.Open("/tmp/quality.index.json")
	if !assert.NoError(t, err) {
		return
	}
	defer f2.Close()
	assert.NoError(t, restore.Load(f2))

	metrics, err := backend.ListMetric(`.*`)
	assert.NoError(t, err)
	assert.Equal(t, len(backend.metrics), len(metrics))

	tagKeys, err := backend.ListTagKeys(`.*`)
	assert.NoError(t, err)
	assert.Equal(t, len(backend.tags.Keys()), len(tagKeys))

	for metric := range backend.metrics {
		iter := backend.tags.Iterator()
		for iter.Next() {
			key, ok := iter.Key().(string)
			if !assert.True(t, ok) {
				return
			}
			value, ok := iter.Value().(*tree.Tree)
			if !assert.True(t, ok) {
				return
			}

			tagValues, err := backend.ListTagValues(key, `.*`)
			assert.NoError(t, err)
			assert.Equal(t, len(value.Values()), len(tagValues))

			iterValue := value.Iterator()
			for iterValue.Next() {
				value, ok := iterValue.Key().(string)
				if !assert.True(t, ok) {
					return
				}

				result1 := backend.Query(metric, []KVPair{
					{key, value},
				}, []Filter{})
				result2 := restore.Query(metric, []KVPair{
					{key, value},
				}, []Filter{})
				assert.Equal(t, result1.Len(), result2.Len())
			}
		}
	}
}

type benchCases struct {
	m Metric
	p []KVPair
	f []Filter
}

func generateBenchmarks(size int, backend *tsIndex) []benchCases {
	var i int
	cases := make([]benchCases, size)
	for i < size {
		for metric := range backend.metrics {
			if strings.Contains(metric.String(), "test") {
				continue
			}
			cases[i] = benchCases{
				m: metric,
				p: []KVPair{},
				f: []Filter{
					{"host", ".*"},
				},
			}
			if i++; i >= size {
				return cases
			}
		}
	}
	if i >= size {
		return cases
	}
	return nil
}

func runBenchmarkAgainstBackend(t assert.TestingT, backend Backend, testcases []benchCases) []ResultSet {
	var results = make([]ResultSet, len(testcases))
	for index, testcase := range testcases {
		res := backend.Query(testcase.m, testcase.p, testcase.f)
		assert.NotNil(t, res)

		results[index] = res
	}
	return results
}

func BenchmarkQueryResults(t *testing.B) {
	if !integration {
		t.SkipNow()
	}
	backend := createIndex()
	if !assert.NoError(t, loadQAData(backend)) {
		return
	}
	testcases := generateBenchmarks(t.N, backend)
	if !assert.NotNil(t, testcases) {
		return
	}
	time.Sleep(time.Second * 10)

	t.ResetTimer()
	runBenchmarkAgainstBackend(t, backend, testcases)
}

func BenchmarkElasticResults(t *testing.B) {
	if !integration {
		t.SkipNow()
	}
	backend := createIndex()
	if !assert.NoError(t, loadQAData(backend)) {
		return
	}
	testcases := generateBenchmarks(t.N, backend)
	if !assert.NotNil(t, testcases) {
		return
	}
	time.Sleep(time.Second * 10)

	client, err := testEsClient()
	if !assert.NoError(t, err) {
		return
	}

	t.ResetTimer()
	runBenchmarkAgainstBackend(t, client, testcases)
}

func printDiff(t *testing.T, only string, s1, s2 ResultSet) {
	diff := s1.Diff(s2)
	if diff.Len() > 0 {
		t.Logf("Only in %s: %v", only, diff)
	}
}

func TestCompareResultsBetweenQAAndIndex(t *testing.T) {
	if !integration {
		t.SkipNow()
	}
	var N = 35

	backend := createIndex()
	if !assert.NoError(t, loadQAData(backend)) {
		return
	}
	testcases := generateBenchmarks(N, backend)

	if !assert.NotNil(t, testcases) {
		return
	}
	time.Sleep(time.Second * 10)

	client, err := testEsClient()
	if !assert.NoError(t, err) {
		return
	}

	qaResults := runBenchmarkAgainstBackend(t, client, testcases)
	localResults := runBenchmarkAgainstBackend(t, backend, testcases)

	var trivial = true
	for index := range qaResults {
		qa, local := qaResults[index], localResults[index]
		assert.Empty(t, qa.Diff(local))
		if !assert.True(t, qa.Equal(local)) {
			t.Logf("QA(%d) != Local(%d)", qa.Len(), local.Len())
			printDiff(t, "QA", qa, local)
			printDiff(t, "Local", local, qa)

			for key := range local.Diff(qa) {
				assert.True(t,
					client.CheckResult(key, testcases[index].m, testcases[index].p, testcases[index].f))
			}
		}
		if len(qa) > 0 {
			trivial = false
		}
	}
	assert.False(t, trivial)
}
