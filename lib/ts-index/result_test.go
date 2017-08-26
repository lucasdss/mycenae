package index

import (
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	arraySize = 128
	testSize  = 10
)

var (
	_ json.Marshaler   = ResultSet{}
	_ json.Unmarshaler = &ResultSet{}
)

func randomResultSet(size int) ResultSet {
	set := makeResultSet()
	for i := 0; i < size; i++ {
		set.Add(ID(rand.Uint64()))
	}
	return set
}

func TestMultipleIntersection(t *testing.T) {
	sets := make([]ResultSet, testSize)
	for i := range sets {
		sets[i] = randomResultSet(arraySize)
		sets[i].Add(ID(3))
	}

	result := Intersection(sets)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)
}

func TestSameIntersection(t *testing.T) {
	sets := make([]ResultSet, testSize)
	same := randomResultSet(arraySize)
	for i := range sets {
		sets[i] = same
	}

	result := Intersection(sets)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)
	assert.Equal(t, same.Len(), result.Len())
}
