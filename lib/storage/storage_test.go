package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"math/rand"
	"testing"
	"time"
)

const (
	keyspace = "test"
	key      = "test"
)

func Test1hPointsPerMinute1Bucket(t *testing.T) {

	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		//t.Logf("current: %v\n", currentTime.UnixNano()/1e6)
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 1 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 1 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}

	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func Test1hPointsPerSecondNumberBuckets(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Second)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 29 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 29 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}
	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func Test4hPointsPerMinuteNumberBktsBktSize(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-4 * time.Hour)
	end := now

	currentTime := start
	for i := 0; i < 240; i++ {
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 2 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 2 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}

	for _, bkt := range strg.getSerie(id).buckets {
		//delta := bkt.points[bkt.index-1].Date - bkt.points[0].Date
		//t.Logf("bucket %v: index: %v start: %v end: %v delta: %v\n", i, bkt.index, bkt.points[0].Date, bkt.points[bkt.index-1].Date, delta)
		if bkt.Index > 120 {
			t.Fatalf("Bucket size bigger than expected: %d", bkt.Index)
		}
		if bkt.Index < 120 {
			t.Fatalf("Bucket size lower than expected: %d", bkt.Index)
		}
	}

	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func BenchmarkInsertPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(keyspace, key, time.Now().Unix()*1e3, rand.Float64())
	}
	b.StopTimer()
}

func BenchmarkReadPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	now := time.Now().Unix() * 1e3
	ptsCount := 1000000

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, time.Now().Unix()*1e3, rand.Float64())
	}

	end := now + int64(ptsCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, now, end, false)
	}
	b.StopTimer()
}

func BenchmarkInsertPointsMultiSeries(b *testing.B) {
	strg := New(nil, nil, nil)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	now := time.Now().Unix() * 1e3
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], now+int64(i), rand.Float64())
	}
	b.StopTimer()

}

func BenchmarkReadPointsMultiSeries(b *testing.B) {
	strg := New(nil, nil, nil)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	now := time.Now().Unix() * 1e3
	ptsCount := 1000000
	for i := 0; i < ptsCount; i++ {
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], now+int64(i), rand.Float64())
	}

	end := now + int64(ptsCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(ks[rand.Intn(3)], k[rand.Intn(3)], now, end, false)
	}
	b.StopTimer()

}

func Test1hGzipPointsPerSecond(t *testing.T) {

	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		//t.Logf("current: %v\n", currentTime.UnixNano()/1e6)
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Second)
	}

	id := strg.id(keyspace, key)

	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)

	pts := strg.getSerie(id).buckets

	err := encoder.Encode(pts)
	if err != nil {
		t.Fatal(err)
	}

	sizeBuckets := binary.Size(b.Bytes())

	var encodePts bytes.Buffer

	zw, err := gzip.NewWriterLevel(&encodePts, 9)
	if err != nil {
		t.Fatal(err)
	}

	compressed, err := zw.Write(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	err = zw.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Buckets Size: %v\n", sizeBuckets)
	t.Logf("Buckets compressed with Gzip Size: %v\n", binary.Size(encodePts.Bytes()))
	t.Logf("Buckets compressed with Gzip wrote size: %v\n", compressed)

}
