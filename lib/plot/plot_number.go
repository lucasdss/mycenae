package plot

import (
	"sort"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/structs"

	"go.uber.org/zap"
)

func (plot *Plot) GetTimeSeries(
	keyspace string,
	keys []string,
	start,
	end int64,
	opers structs.DataOperations,
	ms,
	keepEmpties bool,
) (serie TS, gerr gobol.Error) {

	tsChan := make(chan TS, len(keys))

	for _, key := range keys {
		plot.concTimeseries <- struct{}{}
		go plot.getTimeSerie(
			keyspace,
			key,
			start,
			end,
			ms,
			keepEmpties,
			opers,
			tsChan,
		)
	}

	j := 0

	for range keys {

		t := <-tsChan
		if t.gerr != nil {
			gerr = t.gerr
		}
		if t.Count > 0 {
			j++
		}
		serie.Data = append(serie.Data, t.Data...)

		serie.Total += t.Total
	}

	if gerr != nil {
		return TS{}, gerr
	}

	exec := false
	for _, oper := range opers.Order {
		switch oper {
		case "downsample":
			if serie.Total > 0 && opers.Downsample.Enabled && exec {
				serie.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, serie.Data)
			}
		case "aggregation":
			exec = true
			if j > 1 {
				sort.Sort(serie.Data)
				serie.Data = merge(opers.Merge, keepEmpties, serie.Data)
			}
		case "rate":
			if opers.Rate.Enabled && exec {
				serie.Data = rate(opers.Rate.Options, serie.Data)
			}
		case "filterValue":
			if opers.FilterValue.Enabled && exec {
				serie.Data = filterValues(opers.FilterValue, serie.Data)
			}
		}
	}

	if opers.Downsample.PointLimit && len(serie.Data) > opers.Downsample.TotalPoints {
		serie.Data = basic(opers.Downsample.TotalPoints, serie.Data)
	}
	serie.Count = len(serie.Data)

	return serie, nil
}

func (plot *Plot) getTimeSerie(
	keyspace string,
	key string,
	start int64,
	end int64,
	ms bool,
	keepEmpties bool,
	opers structs.DataOperations,
	tsChan chan TS,
) {

	pts, err := plot.persist.cluster.Read(keyspace, key, start, end)
	if err != nil {
		gblog.Error("", zap.Error(err))
	}

	serie := TS{Data: pts, Total: len(pts)}
	for _, oper := range opers.Order {
		switch oper {
		case "downsample":
			if serie.Total > 0 && opers.Downsample.Enabled {
				serie.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, serie.Data)
			}
		case "aggregation":
			serie.Count = len(serie.Data)
			tsChan <- serie
			<-plot.concTimeseries
			return
		case "rate":
			if opers.Rate.Enabled {
				serie.Data = rate(opers.Rate.Options, serie.Data)
			}
		case "filterValue":
			if opers.FilterValue.Enabled {
				serie.Data = filterValues(opers.FilterValue, serie.Data)

			}
		}
	}

	serie.Count = len(serie.Data)

	tsChan <- serie
	<-plot.concTimeseries
}

func (plot *Plot) getTimeSerieBucket(
	index int,
	keyspace,
	key string,
	start,
	end int64,
	tuuid,
	ms bool,
	bucketChan chan TS,
) {

	resultSet, gerr := plot.persist.cluster.Read(keyspace, key, start, end)

	bucketChan <- TS{
		index: index,
		Total: len(resultSet),
		Data:  resultSet,
		gerr:  gerr,
	}
	<-plot.concReads
}
