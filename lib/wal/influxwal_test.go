package wal_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/golang/snappy"
	"github.com/uol/mycenae/lib/wal"
)

func MustTempDir() string {
	dir, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := ioutil.TempFile(dir, "waltest")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func fatal(t *testing.T, msg string, err error) {
	t.Fatalf("unexpected error %v: %v", msg, err)
}

func TestWALWriter_WriteMulti_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	p1 := wal.NewValue(1, 1.1)
	p2 := wal.NewValue(1, int64(1))
	p3 := wal.NewValue(1, true)
	p4 := wal.NewValue(1, "string")
	p5 := wal.NewValue(1, ^uint64(0))

	values := map[string][]wal.Value{
		"cpu,host=A#!~#float":    []wal.Value{p1},
		"cpu,host=A#!~#int":      []wal.Value{p2},
		"cpu,host=A#!~#bool":     []wal.Value{p3},
		"cpu,host=A#!~#string":   []wal.Value{p4},
		"cpu,host=A#!~#unsigned": []wal.Value{p5},
	}

	entry := &wal.WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*wal.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}

func TestWALWriter_WriteMulti_LargeBatch(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	var points []wal.Value
	for i := 0; i < 100000; i++ {
		points = append(points, wal.NewValue(int64(i), int64(1)))
	}

	values := map[string][]wal.Value{
		"cpu,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
		"mem,host=A,server=01,foo=bar,tag=really-long#!~#float": points,
	}

	entry := &wal.WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*wal.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}
func TestWALWriter_WriteMulti_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	p1 := wal.NewValue(1, int64(1))
	p2 := wal.NewValue(1, int64(2))

	exp := []struct {
		key    string
		values []wal.Value
	}{
		{"cpu,host=A#!~#value", []wal.Value{p1}},
		{"cpu,host=B#!~#value", []wal.Value{p2}},
	}

	for _, v := range exp {
		entry := &wal.WriteWALEntry{
			Values: map[string][]wal.Value{v.key: v.values},
		}

		if err := w.Write(mustMarshalEntry(entry)); err != nil {
			fatal(t, "write points", err)
		}
		if err := w.Flush(); err != nil {
			fatal(t, "flush", err)
		}
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	for _, ep := range exp {
		if !r.Next() {
			t.Fatalf("expected next, got false")
		}

		we, err := r.Read()
		if err != nil {
			fatal(t, "read entry", err)
		}

		e, ok := we.(*wal.WriteWALEntry)
		if !ok {
			t.Fatalf("expected WriteWALEntry: got %#v", e)
		}

		for k, v := range e.Values {
			if got, exp := k, ep.key; got != exp {
				t.Fatalf("key mismatch. got %v, exp %v", got, exp)
			}

			if got, exp := len(v), len(ep.values); got != exp {
				t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
			}

			for i, vv := range v {
				if got, exp := vv.String(), ep.values[i].String(); got != exp {
					t.Fatalf("points mismatch: got %v, exp %v", got, exp)
				}
			}
		}
	}

	if n := r.Count(); n != MustReadFileSize(f) {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, MustReadFileSize(f))
	}
}

func TestWALWriter_WriteDelete_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	entry := &wal.DeleteWALEntry{
		Keys: [][]byte{[]byte("cpu")},
	}

	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*wal.DeleteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	if got, exp := len(e.Keys), len(entry.Keys); got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(e.Keys[0]), string(entry.Keys[0]); got != exp {
		t.Fatalf("key mismatch: got %v, exp %v", got, exp)
	}
}

func TestWALWriter_WriteMultiDelete_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	p1 := wal.NewValue(1, true)
	values := map[string][]wal.Value{
		"cpu,host=A#!~#value": []wal.Value{p1},
	}

	writeEntry := &wal.WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(writeEntry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Write the delete entry
	deleteEntry := &wal.DeleteWALEntry{
		Keys: [][]byte{[]byte("cpu,host=A#!~value")},
	}

	if err := w.Write(mustMarshalEntry(deleteEntry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	// Read the write points first
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*wal.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		if got, exp := len(v), len(values[k]); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	// Read the delete second
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err = r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	de, ok := we.(*wal.DeleteWALEntry)
	if !ok {
		t.Fatalf("expected DeleteWALEntry: got %#v", e)
	}

	if got, exp := len(de.Keys), len(deleteEntry.Keys); got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(de.Keys[0]), string(deleteEntry.Keys[0]); got != exp {
		t.Fatalf("key mismatch: got %v, exp %v", got, exp)
	}
}

func TestWALWriter_WriteMultiDeleteRange_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	p1 := wal.NewValue(1, 1.0)
	p2 := wal.NewValue(2, 2.0)
	p3 := wal.NewValue(3, 3.0)

	values := map[string][]wal.Value{
		"cpu,host=A#!~#value": []wal.Value{p1, p2, p3},
	}

	writeEntry := &wal.WriteWALEntry{
		Values: values,
	}

	if err := w.Write(mustMarshalEntry(writeEntry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Write the delete entry
	deleteEntry := &wal.DeleteRangeWALEntry{
		Keys: [][]byte{[]byte("cpu,host=A#!~value")},
		Min:  2,
		Max:  3,
	}

	if err := w.Write(mustMarshalEntry(deleteEntry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}

	r := wal.NewWALSegmentReader(f)

	// Read the write points first
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*wal.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for k, v := range e.Values {
		if got, exp := len(v), len(values[k]); got != exp {
			t.Fatalf("values length mismatch: got %v, exp %v", got, exp)
		}

		for i, vv := range v {
			if got, exp := vv.String(), values[k][i].String(); got != exp {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}

	// Read the delete second
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err = r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	de, ok := we.(*wal.DeleteRangeWALEntry)
	if !ok {
		t.Fatalf("expected DeleteWALEntry: got %#v", e)
	}

	if got, exp := len(de.Keys), len(deleteEntry.Keys); got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := string(de.Keys[0]), string(deleteEntry.Keys[0]); got != exp {
		t.Fatalf("key mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := de.Min, int64(2); got != exp {
		t.Fatalf("min time mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := de.Max, int64(3); got != exp {
		t.Fatalf("min time mismatch: got %v, exp %v", got, exp)
	}

}

func TestWAL_ClosedSegments(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	zp, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("error initializing zap logger: %v", err)
	}

	s := &wal.Settings{
		PathWAL:            dir,
		SyncInterval:       "1s",
		CleanupInterval:    "1h",
		CheckPointInterval: "10s",
		CheckPointPath:     dir,
		MaxBufferSize:      10000,
		MaxConcWrite:       4,
	}

	w, err := wal.New(s, zp)
	if err != nil {
		t.Fatalf("error initializing WAL: %v", err)
	}
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err := w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}

	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}

	if _, err := w.WriteMulti(map[string][]wal.Value{
		"cpu,host=A#!~#value": []wal.Value{
			wal.NewValue(1, 1.1),
		},
	}); err != nil {
		t.Fatalf("error writing points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("error closing wal: %v", err)
	}

	// Re-open the WAL
	w, err = wal.New(s, zp)
	if err != nil {
		t.Fatalf("error re-initializing WAL: %v", err)
	}
	defer w.Close()
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err = w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}
	if got, exp := len(files), 1; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}
}

func TestWAL_Delete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	zp, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("error initializing zap logger: %v", err)
	}

	s := &wal.Settings{
		PathWAL:            dir,
		SyncInterval:       "1s",
		CleanupInterval:    "1h",
		CheckPointInterval: "10s",
		CheckPointPath:     dir,
		MaxBufferSize:      10000,
		MaxConcWrite:       4,
	}

	w, err := wal.New(s, zp)
	if err != nil {
		t.Fatalf("error initializing WAL: %v", err)
	}
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err := w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}

	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}

	if _, err := w.Delete([][]byte{[]byte("cpu")}); err != nil {
		t.Fatalf("error writing points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("error closing wal: %v", err)
	}

	// Re-open the WAL
	w, err = wal.New(s, zp)
	if err != nil {
		t.Fatalf("error re-initializing WAL: %v", err)
	}
	defer w.Close()
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err = w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}
	if got, exp := len(files), 1; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}
}

func TestWALWriter_Corrupt(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)
	corruption := []byte{1, 4, 0, 0, 0}

	p1 := wal.NewValue(1, 1.1)
	values := map[string][]wal.Value{
		"cpu,host=A#!~#float": []wal.Value{p1},
	}

	entry := &wal.WriteWALEntry{
		Values: values,
	}
	if err := w.Write(mustMarshalEntry(entry)); err != nil {
		fatal(t, "write points", err)
	}

	if err := w.Flush(); err != nil {
		fatal(t, "flush", err)
	}

	// Write some random bytes to the file to simulate corruption.
	if _, err := f.Write(corruption); err != nil {
		fatal(t, "corrupt WAL segment", err)
	}

	// Create the WAL segment reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		fatal(t, "seek", err)
	}
	r := wal.NewWALSegmentReader(f)

	// Try to decode two entries.

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}
	if _, err := r.Read(); err != nil {
		fatal(t, "read entry", err)
	}

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}
	if _, err := r.Read(); err == nil {
		fatal(t, "read entry did not return err", nil)
	}

	// Count should only return size of valid data.
	expCount := MustReadFileSize(f) - int64(len(corruption))
	if n := r.Count(); n != expCount {
		t.Fatalf("wrong count of bytes read, got %d, exp %d", n, expCount)
	}
}

func TestWriteWALSegment_UnmarshalBinary_WriteWALCorrupt(t *testing.T) {
	p1 := wal.NewValue(1, 1.1)
	p2 := wal.NewValue(1, int64(1))
	p3 := wal.NewValue(1, true)
	p4 := wal.NewValue(1, "string")
	p5 := wal.NewValue(1, uint64(1))

	values := map[string][]wal.Value{
		"cpu,host=A#!~#float":    []wal.Value{p1, p1},
		"cpu,host=A#!~#int":      []wal.Value{p2, p2},
		"cpu,host=A#!~#bool":     []wal.Value{p3, p3},
		"cpu,host=A#!~#string":   []wal.Value{p4, p4},
		"cpu,host=A#!~#unsigned": []wal.Value{p5, p5},
	}

	w := &wal.WriteWALEntry{
		Values: values,
	}

	b, err := w.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error, got %v", err)
	}

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		if err != nil && err != wal.ErrWALCorrupt {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestWriteWALSegment_UnmarshalBinary_DeleteWALCorrupt(t *testing.T) {
	w := &wal.DeleteWALEntry{
		Keys: [][]byte{[]byte("foo"), []byte("bar")},
	}

	b, err := w.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error, got %v", err)
	}

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		if err != nil && err != wal.ErrWALCorrupt {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestWriteWALSegment_UnmarshalBinary_DeleteRangeWALCorrupt(t *testing.T) {
	w := &wal.DeleteRangeWALEntry{
		Keys: [][]byte{[]byte("foo"), []byte("bar")},
		Min:  1,
		Max:  2,
	}

	b, err := w.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error, got %v", err)
	}

	// Test every possible truncation of a write WAL entry
	for i := 0; i < len(b); i++ {
		// re-allocated to ensure capacity would be exceed if slicing
		truncated := make([]byte, i)
		copy(truncated, b[:i])
		err := w.UnmarshalBinary(truncated)
		if err != nil && err != wal.ErrWALCorrupt {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkWALSegmentWriter(b *testing.B) {
	points := map[string][]wal.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], wal.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	write := &wal.WriteWALEntry{
		Values: points,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(mustMarshalEntry(write)); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}
}

func BenchmarkWALSegmentReader(b *testing.B) {
	points := map[string][]wal.Value{}
	for i := 0; i < 5000; i++ {
		k := "cpu,host=A#!~#value"
		points[k] = append(points[k], wal.NewValue(int64(i), 1.1))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	write := &wal.WriteWALEntry{
		Values: points,
	}

	for i := 0; i < 100; i++ {
		if err := w.Write(mustMarshalEntry(write)); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}

	r := wal.NewWALSegmentReader(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f.Seek(0, io.SeekStart)
		b.StartTimer()

		for r.Next() {
			_, err := r.Read()
			if err != nil {
				b.Fatalf("unexpected error reading entry: %v", err)
			}
		}
	}
}

// MustReadFileSize returns the size of the file, or panics.
func MustReadFileSize(f *os.File) int64 {
	stat, err := os.Stat(f.Name())
	if err != nil {
		panic(fmt.Sprintf("failed to get size of file at %s: %s", f.Name(), err.Error()))
	}
	return stat.Size()
}

func mustMarshalEntry(entry wal.WALEntry) (wal.WalEntryType, []byte) {
	bytes := make([]byte, 1024<<2)

	b, err := entry.Encode(bytes)
	if err != nil {
		panic(fmt.Sprintf("error encoding: %v", err))
	}

	return entry.Type(), snappy.Encode(b, b)
}
