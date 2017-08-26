package plot

import (
	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/structs"
)

func (plot Plot) GetGroups(filters []structs.TSDBfilter, tsobs []meta.TSDBData) (groups [][]meta.TSDBData) {

	if len(tsobs) == 0 {
		return groups
	}

	groups = append(groups, []meta.TSDBData{tsobs[0]})
	tsobs = append(tsobs[:0], tsobs[1:]...)
	deleted := 0

	for i := range tsobs {

		in := true

		j := i - deleted

		for k, group := range groups {

			in = true

			for _, filter := range filters {

				if !filter.GroupBy {
					continue
				}

				if group[0].Tags[filter.Tagk] != tsobs[0].Tags[filter.Tagk] {
					in = false
				}
			}

			if in {
				groups[k] = append(groups[k], tsobs[0])
				tsobs = append(tsobs[:j], tsobs[j+1:]...)
				deleted++
				break
			}

		}

		if !in {
			groups = append(groups, []meta.TSDBData{tsobs[0]})
			tsobs = append(tsobs[:j], tsobs[j+1:]...)
			deleted++
		}

	}

	return groups
}
