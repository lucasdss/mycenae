package index

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sync"

	tree "github.com/emirpasic/gods/trees/redblacktree"
)

type tsIndex struct {
	sync.RWMutex
	tags    *tree.Tree
	metrics map[Metric]ResultSet

	metricTags map[Metric][]string
	timeseries ResultSet
}

func createIndex() *tsIndex {
	return &tsIndex{
		metrics: make(map[Metric]ResultSet),
		tags:    tree.NewWithStringComparator(),

		timeseries: ResultSet{},
	}
}

// Create returns a new backend using the tsIndex private structure
func Create() Backend {
	return createIndex()
}

func (i *tsIndex) Add(m Metric, p []KVPair, id ID) error {
	i.Lock()
	defer i.Unlock()

	i.timeseries.Add(id)
	if _, ok := i.metrics[m]; !ok {
		i.metrics[m] = makeResultSet()
	}
	i.metrics[m].Add(id)
	for _, pair := range p {
		var st *tree.Tree
		content, found := i.tags.Get(pair.Key)
		if !found {
			st = tree.NewWithStringComparator()
			i.tags.Put(pair.Key, st)
		} else {
			var ok bool
			st, ok = content.(*tree.Tree)
			if !ok {
				return fmt.Errorf("Error with types")
			}
		}

		var result ResultSet
		content, found = st.Get(pair.Value)
		if !found {
			result = makeResultSet()
			st.Put(pair.Value, result)
		} else {
			var ok bool
			result, ok = content.(ResultSet)
			if !ok {
				return fmt.Errorf("Error with types")
			}
		}
		result.Add(id)
	}
	return nil
}

func (i *tsIndex) Exists(id ID) (bool, error) {
	return i.timeseries.Has(id), nil
}

func (i *tsIndex) ListMetric(expr string) ([]string, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}

	var answ []string
	for key := range i.metrics {
		if re.MatchString(key.String()) {
			answ = append(answ, key.String())
		}
	}
	return answ, nil
}

func (i *tsIndex) ListTagKeys(expr string) ([]string, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}

	var answ []string
	for _, key := range i.tags.Keys() {
		s, ok := key.(string)
		if ok && re.MatchString(s) {
			answ = append(answ, s)
		}
	}
	return answ, nil
}

func (i *tsIndex) ListTagValues(tagkey, expr string) ([]string, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}

	values, found := i.tags.Get(tagkey)
	if !found {
		return make([]string, 0), nil
	}

	subtree, ok := values.(*tree.Tree)
	if !ok {
		return nil, fmt.Errorf("Internal consistency error")
	}
	var answ []string

	for _, key := range subtree.Keys() {
		keyValue, ok := key.(string)
		if ok && re.MatchString(keyValue) {
			answ = append(answ, keyValue)
		}
	}
	return answ, nil
}

func (i *tsIndex) Query(m Metric, ps []KVPair, fs []Filter) ResultSet {
	i.RLock()
	defer i.RUnlock()

	rs, ok := i.metrics[m]
	if !ok {
		return emptySet
	}

	sets := make([]ResultSet, len(ps))
	for index := range sets {
		values, found := i.tags.Get(ps[index].Key)
		if !found {
			return emptySet
		}

		valuesTree, ok := values.(*tree.Tree)
		if !ok {
			return emptySet
		}

		content, found := valuesTree.Get(ps[index].Value)
		if !found {
			return emptySet
		}

		sets[index], ok = content.(ResultSet)
		if !ok {
			return emptySet
		}
	}

	var filterResult ResultSet
	for _, filter := range fs {
		generic, found := i.tags.Get(filter.Key)
		if !found {
			return emptySet
		}

		values, ok := generic.(*tree.Tree)
		if !ok {
			return emptySet
		}

		iter := values.Iterator()
		result, err := filter.Run(iter.Next, func() (string, ResultSet) {
			val, ok := iter.Key().(string)
			if !ok {
				return "", emptySet
			}

			result, ok := iter.Value().(ResultSet)
			if !ok {
				return val, emptySet
			}
			return val, result
		})
		if err != nil {
			return emptySet
		}
		if filterResult == nil {
			filterResult = result
		} else {
			filterResult = singleIntersect(filterResult, result)
		}
	}
	if filterResult == nil {
		filterResult = emptySet
	}
	if len(fs) > 0 {
		rs = singleIntersect(rs, filterResult)
	}
	if len(ps) > 0 {
		rs = singleIntersect(rs, Intersection(sets))
	}
	return rs
}

type format struct {
	Metric map[Metric]ResultSet            `json:"metrics"`
	Tags   map[string]map[string]ResultSet `json:"tags"`
}

func (i *tsIndex) Store(writer io.Writer) error {
	i.Lock()
	defer i.Unlock()

	content := format{
		Metric: i.metrics,
		Tags:   make(map[string]map[string]ResultSet),
	}

	iter := i.tags.Iterator()
	for iter.Next() {
		key, ok := iter.Key().(string)
		if !ok {
			return fmt.Errorf("Invalid key in data structure")
		}

		value, ok := iter.Value().(*tree.Tree)
		if !ok {
			return fmt.Errorf("Invalid key in data structure")
		}

		keyMap := make(map[string]ResultSet)
		content.Tags[key] = keyMap

		keyIter := value.Iterator()
		for keyIter.Next() {
			key1, ok := keyIter.Key().(string)
			if !ok {
				return fmt.Errorf("Invalid key in data structure")
			}

			value1, ok := keyIter.Value().(ResultSet)
			if !ok {
				return fmt.Errorf("Invalid key in data structure")
			}

			keyMap[key1] = value1
		}
	}
	return json.NewEncoder(writer).Encode(&content)
}

func (i *tsIndex) Load(reader io.Reader) error {
	var content format

	err := json.NewDecoder(reader).Decode(&content)
	if err != nil {
		return err
	}

	i.metrics = content.Metric
	i.tags = tree.NewWithStringComparator()
	for key, value := range content.Tags {
		tr := tree.NewWithStringComparator()
		i.tags.Put(key, tr)
		for key1, value1 := range value {
			tr.Put(key1, value1)
		}
	}
	return nil
}
