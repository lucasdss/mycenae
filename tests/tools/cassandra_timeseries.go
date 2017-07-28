package tools

import (
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"sort"
	"time"

	"github.com/gocql/gocql"
)

type cassTs struct {
	cql *gocql.Session
}

func (ts *cassTs) init(cql *gocql.Session) {
	ts.cql = cql
}

func (ts *cassTs) GetValueFromIDSTAMP(keyspace, id string) (nValue float64) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT value FROM %s.timeseries WHERE id=?`, keyspace),
		id,
	).Scan(&nValue); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	return
}

func (ts *cassTs) GetValueFromDateSTAMP(keyspace, id string, date time.Time) (nValue float64) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT value FROM %s.timeseries WHERE id=? AND date=?`, keyspace),
		id,
		date,
	).Scan(&nValue); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	return
}

func (ts *cassTs) GetTextFromDateSTAMP(keyspace, id string, date time.Time) (nValue string) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT value FROM %s.ts_text_stamp WHERE id=? AND date=?`, keyspace),
		id,
		date.Truncate(time.Second),
	).Scan(&nValue); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	return
}

func (ts *cassTs) CountValueFromIDSTAMP(keyspace, id string) (count int) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT count(*) FROM %s.timeseries WHERE id=?`, keyspace),
		id,
	).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ts *cassTs) CountTextFromIDSTAMP(keyspace, id string) (count int) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT count(*) FROM %s.ts_text_stamp WHERE id=?`, keyspace),
		id,
	).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ts *cassTs) GetValueTTLDaysFromDateSTAMP(id string, date time.Time) (days float64) {
	var seconds int
	if err := ts.cql.Query(`SELECT ttl(value) FROM ts_number_stamp WHERE id=? AND date=?`,
		id,
		date,
	).Scan(&seconds); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	days = math.Ceil(float64(seconds) / 60 / 60 / 24)
	return
}

func (ts *cassTs) GetValueFromTwoDatesSTAMP(keyspace, id string, dateBeforeRequest time.Time, dateAfterRequest time.Time) (nValue float64) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT value FROM %s.timeseries WHERE id=? AND date >= ? AND date <= ?`, keyspace),
		id,
		dateBeforeRequest,
		dateAfterRequest,
	).Scan(&nValue); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	return
}

func (ts *cassTs) GetTextFromTwoDatesSTAMP(keyspace, id string, dateBeforeRequest time.Time, dateAfterRequest time.Time) (nValue string) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT value FROM %s.ts_text_stamp WHERE id=? AND date >= ? AND date <= ?`, keyspace),
		id,
		dateBeforeRequest.Truncate(time.Second),
		dateAfterRequest.Truncate(time.Second),
	).Scan(&nValue); err != nil && err != gocql.ErrNotFound {
		log.Println(err)
	}
	return
}

func (ts *cassTs) CountValueFromIDAndDateSTAMP(keyspace, id string, date time.Time) (count int) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT count(*) FROM %s.timeseries WHERE id=? AND date=?`, keyspace),
		id,
		date,
	).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ts *cassTs) CountTextFromIDAndDateSTAMP(keyspace, id string, date time.Time) (count int) {
	if err := ts.cql.Query(fmt.Sprintf(`SELECT count(*) FROM %s.ts_text_stamp WHERE id=? AND date=?`, keyspace),
		id,
		date.Truncate(time.Second),
	).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ts *cassTs) GetHashFromMetricAndTags(metric string, tags map[string]string) string {
	h := crc32.NewIEEE()
	h.Write([]byte(metric))
	mk := []string{}

	for k := range tags {
		if k != "ksid" && k != "ttl" && k != "tuuid" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {
		h.Write([]byte(k))
		h.Write([]byte(tags[k]))
	}

	return fmt.Sprint(h.Sum32())
}

func (ts *cassTs) GetTextHashFromMetricAndTags(metric string, tags map[string]string) string {
	h := crc32.NewIEEE()
	h.Write([]byte(metric))
	mk := []string{}

	for k := range tags {
		if k != "ksid" && k != "ttl" && k != "tuuid" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {
		h.Write([]byte(k))
		h.Write([]byte(tags[k]))
	}

	return fmt.Sprint("T", h.Sum32())
}
