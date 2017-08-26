package index

import "regexp"

// Filter is a regexp filter
type Filter struct {
	Key        string `json:"key"`
	Expression string `json:"regexp"`
}

// Run executes the expression filter
func (f Filter) Run(has func() bool, next func() (string, ResultSet)) (ResultSet, error) {
	var answ ResultSet
	re, err := regexp.Compile(f.Expression)
	if err != nil {
		return emptySet, err
	}

	for has() {
		value, set := next()
		if re.MatchString(value) {
			if answ == nil {
				answ = makeResultSet()
				for id := range set {
					answ.Add(id)
				}
			} else {
				answ = singleUnion(answ, set)
			}
		}
	}
	return answ, nil
}
