package depot

import "github.com/uol/gobol"

// Persistence interface abstracts where we save data
type Persistence interface {
	Read(ksid, tsid string, blkid int64) ([]byte, gobol.Error)
	Write(ksid, tsid string, blkid int64, points []byte) gobol.Error
}
