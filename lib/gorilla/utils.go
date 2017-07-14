package gorilla

import (
	"errors"
	"time"
)

const (
	hour       = 3600
	day        = 24 * hour
	maxBlocks  = 12
	headerSize = 19
)

// BlockID returns an UTC timestamp at
// the head of the even hour based on the
// given timestamp
func BlockID(timestamp int64) int64 {
	now := time.Unix(timestamp, 0).UTC()

	_, m, s := now.Clock()
	now = now.Add(-(time.Duration(m) * time.Minute) - (time.Duration(s) * time.Second))

	if now.Hour()%2 == 0 {
		return now.Unix()
	}

	return now.Unix() - hour
}

func getIndex(timestamp int64) int {

	return time.Unix(timestamp, 0).UTC().Hour() / 2

}

// MilliToSeconds convert timestamp in milliseconds to timestamp in seconds.
// An error is raised if the given timestamp is greater than 13 numbers.
func MilliToSeconds(t int64) (int64, error) {
	msTime := t

	i := 0
	for {
		msTime = msTime / 10
		if msTime == 0 {
			break
		}
		i++
	}

	if i > 13 {
		return t, errors.New("the maximum resolution suported for timestamp is milliseconds")
	}

	if i > 10 {
		return t / 1000, nil
	}

	return t, nil
}
