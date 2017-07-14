package plot

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/lib/gorilla"
)

var _ json.Marshaler = &TSMarshaler{}

const (
	dataSize = 1024 * 15
)

func generateData() gorilla.Pnts {
	now := time.Now()
	data := make(gorilla.Pnts, dataSize)
	for i := range data {
		data[i].Date = now.Add(time.Duration(i) * time.Minute).Unix()
		data[i].Empty = (i%9 == 0)
		data[i].Value = 32.7
	}
	return data
}

func oldMarshaler(data gorilla.Pnts, fill string) ([]byte, error) {
	points := map[string]interface{}{}
	for _, point := range data {
		k := point.Date

		ksrt := strconv.FormatInt(k, 10)
		if point.Empty {
			switch fill {
			case "null":
				points[ksrt] = nil
			case "nan":
				points[ksrt] = "NaN"
			default:
				points[ksrt] = point.Value
			}
		} else {
			points[ksrt] = point.Value
		}
	}
	return json.Marshal(points)
}

func newMarshaler(data gorilla.Pnts, fill string) ([]byte, error) {
	m := &TSMarshaler{
		fill:  fill,
		milli: false,
		data:  data,
	}
	return m.MarshalJSON()
}

func TestMarshaller(t *testing.T) {
	for _, fill := range []string{"null", "nan", "abdula"} {
		data := generateData()

		new, err := newMarshaler(data, fill)
		assert.NoError(t, err)
		assert.NotEmpty(t, new)

		old, err := oldMarshaler(data, fill)
		assert.NoError(t, err)
		assert.NotEmpty(t, old)

		assert.Equal(t, old, new)
	}
}

type marshaler func(data gorilla.Pnts, fill string) ([]byte, error)

func templateBenchmark(t *testing.B, f marshaler) {
	data := make([]gorilla.Pnts, t.N)
	for i := range data {
		data[i] = generateData()
	}
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		response, err := f(data[i], "null")
		assert.NoError(t, err)
		assert.NotEmpty(t, response)
	}
}

func BenchmarkOldMarshaler(t *testing.B) {
	templateBenchmark(t, oldMarshaler)
}

func BenchmarkNewMarshaler(t *testing.B) {
	templateBenchmark(t, newMarshaler)
}
