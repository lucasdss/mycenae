package gorilla

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/tserr"
)

type TSDBpoints []TSDBpoint

type TSDBpoint struct {
	Metric    string            `json:"metric,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	Value     *float32          `json:"value,omitempty"`
	Text      string            `json:"text,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (p TSDBpoints) Validate() gobol.Error {
	if len(p) == 0 {
		return tserr.New(
			errors.New("no points"),
			"no points",
			http.StatusBadRequest,
			map[string]interface{}{
				"package": "Collector",
				"struct":  "TSDBpoints",
			},
		)
	}
	return nil
}

type Pnt struct {
	Date  int64
	Value float32
	Empty bool
}

type Pnts []*pb.Point

func (s Pnts) Len() int {
	return len(s)
}

func (s Pnts) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func (s Pnts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type Point struct {
	Message   TSDBpoint
	ID        string
	KsID      string
	Timestamp int64
	Number    bool
}
