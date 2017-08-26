package index

type operatorWrapper struct {
	Must    []interface{} `json:"must,omitempty"`
	MustNot []interface{} `json:"must_not,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

type boolWrapper struct {
	Bool operatorWrapper `json:"bool"`
}

type queryWrapper struct {
	Size   int64       `json:"size,omitempty"`
	From   int64       `json:"from,omitempty"`
	Query  boolWrapper `json:"filter"`
	Fields []string    `json:"fields,omitempty"`
}

type esNested struct {
	Path      string      `json:"path"`
	ScoreMode string      `json:"score_mode,omitempty"`
	Query     boolWrapper `json:"filter"`
}

type esNestedQuery struct {
	Nested esNested `json:"nested"`
}

type esTerm struct {
	Term map[string]string `json:"term"`
}

type esRegexp struct {
	Regexp map[string]string `json:"regexp"`
}
