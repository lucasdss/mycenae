package index

import "encoding/json"

// ResultSet is the result of an index query
type ResultSet map[ID]bool

func makeResultSet() ResultSet {
	return make(ResultSet)
}

var emptySet = makeResultSet()

// Len is the number of elements in the collection.
func (s ResultSet) Len() int {
	return len(s)
}

// Has checks if a result set contains a given element
func (s ResultSet) Has(id ID) bool {
	_, ok := s[id]
	return ok
}

// Add adds an element to the list
func (s ResultSet) Add(id ID) {
	s[id] = true
}

// Equal checks for set equality
func (s ResultSet) Equal(other ResultSet) bool {
	if s.Len() != other.Len() {
		return false
	}
	for key, value := range s {
		if val2, ok := other[key]; !ok || value != val2 {
			return false
		}
	}
	return true
}

// MarshalJSON transforms a result set into json representation
func (s ResultSet) MarshalJSON() ([]byte, error) {
	var (
		i      = 0
		format = make([]ID, len(s))
	)
	for key := range s {
		format[i] = key
		i++
	}
	return json.Marshal(format)
}

// UnmarshalJSON transforms a json representation into a result set
func (s *ResultSet) UnmarshalJSON(data []byte) error {
	var format []ID

	if err := json.Unmarshal(data, &format); err != nil {
		return err
	}
	*s = makeResultSet()
	for _, id := range format {
		s.Add(id)
	}
	return nil
}

func singleIntersect(s1, s2 ResultSet) ResultSet {
	var (
		answ = makeResultSet()
	)

	for key := range s1 {
		if s2.Has(key) {
			answ[key] = true
		}
	}
	return answ
}

// Intersection caltulates the intersection between multiple result sets
func Intersection(s []ResultSet) ResultSet {
	if s == nil || len(s) == 0 {
		return emptySet
	}
	if len(s) == 1 {
		return s[0]
	}
	return singleIntersect(s[0], Intersection(s[1:]))
}

func singleUnion(s1, s2 ResultSet) ResultSet {
	answ := makeResultSet()
	for key := range s1 {
		answ.Add(key)
	}
	for key := range s2 {
		answ.Add(key)
	}
	return answ
}

// Union calculates the union between multiple result sets
func Union(s []ResultSet) ResultSet {
	if s == nil || len(s) == 0 {
		return emptySet
	}
	if len(s) == 1 {
		return s[0]
	}
	return singleUnion(s[0], Union(s[1:]))
}

// Diff returns the set difference of result sets
func (s ResultSet) Diff(other ResultSet) ResultSet {
	r := makeResultSet()
	for key := range s {
		if _, ok := other[key]; !ok {
			r[key] = true
		}
	}
	return r
}
