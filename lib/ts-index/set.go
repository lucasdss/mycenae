package index

import (
	"sync"

	"go.uber.org/zap"
)

// setKey is a key to use in the set lookup
type setKey struct {
	index, _type string
}

// Set holds a group of indexes
type Set struct {
	indexes  map[setKey]Backend
	generate func(index, itype string) Backend // generate should generate missing backend

	sync.RWMutex
}

// CreateSet creates a set of indexes
func CreateSet() *Set {
	return &Set{indexes: make(map[setKey]Backend)}
}

// CreateElasticSet creates a set of indexes that generate elastic clients
func CreateElasticSet(host string, logger *zap.Logger) *Set {
	set := CreateSet()
	set.generate = func(index, itype string) Backend {
		backend, _ := createESIndex(host, index, itype, logger)
		return backend
	}
	return set
}

// Get retrieves a specific index
func (s *Set) Get(index, itype string) Backend {
	s.RLock()
	defer s.RUnlock()

	key := setKey{index: index, _type: itype}
	retrieve, ok := s.indexes[key]
	if ok {
		return retrieve
	}
	if s.generate != nil {
		gen := s.generate(index, itype)
		s.indexes[key] = gen
		return gen
	}
	return nil
}

// Add adds an index to the set
func (s *Set) Add(index, itype string, backend Backend) {
	s.Lock()
	defer s.Unlock()

	key := setKey{index: index, _type: itype}
	s.indexes[key] = backend
}

// Delete deletes an index
func (s *Set) Delete(index, itype string) {
	s.Lock()
	defer s.Unlock()

	key := setKey{index: index, _type: itype}
	delete(s.indexes, key)
}
