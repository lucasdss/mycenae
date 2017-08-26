package index

import (
	"io"
	"strconv"

	pb "github.com/uol/mycenae/lib/proto"
)

// Metric refers to a timeseries metric
type Metric string

func (m Metric) String() string {
	return string(m)
}

// KVPair is a key-value pair for tags
type KVPair struct {
	Key   string
	Value string
}

// ParseTags convert from the protobuf format to the internal representation
func ParseTags(tags []*pb.Tag) []KVPair {
	convert := make([]KVPair, len(tags))
	for index, tag := range tags {
		convert[index].Key = tag.GetKey()
		convert[index].Value = tag.GetValue()
	}
	return convert
}

// ID is a unique timeseries identifier
type ID uint64

func (id ID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

// ParseID parses the ID into a number
func ParseID(id string) ID {
	val, _ := strconv.ParseUint(id, 10, 64)
	return ID(val)
}

// Backend defines the behaviour of the index
type Backend interface {
	// Add adds a new document to the index
	Add(Metric, []KVPair, ID) error
	// Query queries the underling index
	Query(Metric, []KVPair, []Filter) ResultSet
	// Exists checks whether an ID exists
	Exists(ID) (bool, error)

	// ListMetric lists all available metrics
	ListMetric(string) ([]string, error)
	// ListTagKeys lists all tag keys from the index
	ListTagKeys(string) ([]string, error)
	// ListTagValues lists all tag values given a tag key and metric, and a regexp
	ListTagValues(string, string) ([]string, error)

	// Store will save the content of the index in a file
	Store(io.Writer) error
	// Load will load data from a file
	Load(io.Reader) error
}
