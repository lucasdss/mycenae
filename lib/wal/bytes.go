package wal

import "sync"

// Bytes is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.
type Bytes struct {
	pool chan []byte
}

// NewBytes returns a Bytes pool with capacity for max byte slices
// to be pool.
func NewBytes(max int) *Bytes {
	return &Bytes{
		pool: make(chan []byte, max),
	}
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *Bytes) Get(sz int) []byte {
	var c []byte
	select {
	case c = <-p.pool:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.
func (p *Bytes) Put(c []byte) {
	select {
	case p.pool <- c:
	default:
	}
}

// LimitedBytes is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.  The pool will
// hold onto a fixed number of byte slices of a maximum size.  If the pool
// is empty and max pool size has not been allocated yet, it will return a
// new byte slice.  Byte slices added to the pool that are over the max size
// are dropped.
type LimitedBytes struct {
	allocated int64
	maxSize   int
	pool      chan []byte
}

// NewBytes returns a Bytes pool with capacity for max byte slices
// to be pool.
func NewLimitedBytes(capacity int, maxSize int) *LimitedBytes {
	return &LimitedBytes{
		pool:    make(chan []byte, capacity),
		maxSize: maxSize,
	}
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *LimitedBytes) Get(sz int) []byte {
	var c []byte

	// If we have not allocated our capacity, return a new allocation,
	// otherwise block until one frees up.
	select {
	case c = <-p.pool:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.  If the byte slice is over the configured max size
// of any byte slice in the pool, it is discared.
func (p *LimitedBytes) Put(c []byte) {
	// Drop buffers that are larger than the max size
	if cap(c) >= p.maxSize {
		return
	}

	select {
	case p.pool <- c:
	default:
	}
}

// Generic is a pool of types that can be re-used.  Items in
// this pool will not be garbage collected when not in use.
type Generic struct {
	pool chan interface{}
	fn   func(sz int) interface{}
}

// NewGeneric returns a Generic pool with capacity for max items
// to be pool.
func NewGeneric(max int, fn func(sz int) interface{}) *Generic {
	return &Generic{
		pool: make(chan interface{}, max),
		fn:   fn,
	}
}

// Get returns a item from the pool or a new instance if the pool
// is empty.  Items returned may not be in the zero state and should
// be reset by the caller.
func (p *Generic) Get(sz int) interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.fn(sz)
	}

	return c
}

// Put returns an item back to the pool.  If the pool is full, the item
// is discarded.
func (p *Generic) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
	}
}

var bufPool sync.Pool

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) *[]byte {
	x := bufPool.Get()
	if x == nil {
		b := make([]byte, size)
		return &b
	}
	buf := x.(*[]byte)
	if cap(*buf) < size {
		b := make([]byte, size)
		return &b
	}
	*buf = (*buf)[:size]
	return buf
}

// putBuf returns a buffer to the pool.
func putBuf(buf *[]byte) {
	bufPool.Put(buf)
}
