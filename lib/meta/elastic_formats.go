package meta

type elasticTerm struct {
	Term map[string]string `json:"term"`
}

type elasticRegexp struct {
	Regexp map[string]string `json:"regexp"`
}

type elasticOperatorWrapper struct {
	Must    []interface{} `json:"must,omitempty"`
	MustNot []interface{} `json:"must_not,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

type elasticBoolWrapper struct {
	Bool elasticOperatorWrapper `json:"bool"`
}

type elasticQueryWrapper struct {
	Size   int64              `json:"size,omitempty"`
	From   int64              `json:"from,omitempty"`
	Query  elasticBoolWrapper `json:"filter"`
	Fields []string           `json:"fields,omitempty"`
}

type elasticResponseCounters struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type elasticResponseTag struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   elasticResponseCounters `json:"_shards"`
	Hits     struct {
		Total    int     `json:"total"`
		MaxScore float32 `json:"max_score"`
		Hits     []struct {
			Index   string  `json:"_index"`
			Type    string  `json:"_type"`
			ID      string  `json:"_id"`
			Version int     `json:"_version,omitempty"`
			Found   bool    `json:"found,omitempty"`
			Score   float32 `json:"_score"`
			Source  struct {
				Key string `json:"key"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type elasticResponseMetric struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   elasticResponseCounters `json:"_shards"`
	Hits     struct {
		Total    int     `json:"total"`
		MaxScore float32 `json:"max_score"`
		Hits     []struct {
			Index   string  `json:"_index"`
			Type    string  `json:"_type"`
			ID      string  `json:"_id"`
			Version int     `json:"_version,omitempty"`
			Found   bool    `json:"found,omitempty"`
			Score   float32 `json:"_score"`
			Source  struct {
				Name string `json:"name"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type elasticResponseTagKey struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   elasticResponseCounters `json:"_shards"`
	Hits     struct {
		Total    int     `json:"total"`
		MaxScore float32 `json:"max_score"`
		Hits     []struct {
			Index   string  `json:"_index"`
			Type    string  `json:"_type"`
			ID      string  `json:"_id"`
			Version int     `json:"_version,omitempty"`
			Found   bool    `json:"found,omitempty"`
			Score   float32 `json:"_score"`
			Source  struct {
				Key string `json:"key"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type elasticNestedQuery struct {
	Nested elasticNested `json:"nested"`
}

type elasticNested struct {
	Path      string             `json:"path"`
	ScoreMode string             `json:"score_mode,omitempty"`
	Query     elasticBoolWrapper `json:"filter"`
}

type elasticResponseMeta struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   elasticResponseCounters `json:"_shards"`
	Hits     struct {
		Total    int     `json:"total"`
		MaxScore float32 `json:"max_score"`
		Hits     []struct {
			Index   string  `json:"_index"`
			Type    string  `json:"_type"`
			ID      string  `json:"_id"`
			Version int     `json:"_version,omitempty"`
			Found   bool    `json:"found,omitempty"`
			Score   float32 `json:"_score"`
			Source  struct {
				Metric string `json:"metric"`
				ID     string `json:"id"`
				Tags   []struct {
					Key   string `json:"tagKey"`
					Value string `json:"tagValue"`
				} `json:"tagsNested"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type elasticResponseTagValue struct {
	Took     int                     `json:"took"`
	TimedOut bool                    `json:"timed_out"`
	Shards   elasticResponseCounters `json:"_shards"`
	Hits     struct {
		Total    int     `json:"total"`
		MaxScore float32 `json:"max_score"`
		Hits     []struct {
			Index   string  `json:"_index"`
			Type    string  `json:"_type"`
			ID      string  `json:"_id"`
			Version int     `json:"_version,omitempty"`
			Found   bool    `json:"found,omitempty"`
			Score   float32 `json:"_score"`
			Source  struct {
				Value string `json:"value"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}
