package plot

import (
	"math"
	"time"

	"github.com/uol/mycenae/lib/gorilla"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/structs"
)

const (
	second = 1
	minute = 60 * second
	hour   = 60 * minute
	day    = 24 * hour
	week   = 7 * day
)

func basic(totalPoints int, serie gorilla.Pnts) (groupSerie gorilla.Pnts) {

	total := len(serie)

	group := float32(total) / float32(totalPoints)

	group = round(group, .5, 0)

	var counter float32

	var avgCounter float32

	var nilCounter float32

	var groupDate int64

	var groupValue float32

	for i, point := range serie {

		groupDate += point.Date

		if point.Empty {

			nilCounter++

		} else {

			groupValue += point.Value

			avgCounter++

		}

		counter++

		if counter == group || i == total-1 {

			groupDate = groupDate / int64(counter)

			groupValue = groupValue / avgCounter

			groupPoint := &pb.Point{}

			groupPoint.Date = groupDate

			groupPoint.Value = groupValue

			if nilCounter == counter {
				groupPoint.Empty = true
			}

			groupSerie = append(groupSerie, groupPoint)

			counter = 0

			avgCounter = 0

			nilCounter = 0

			groupDate = 0

			groupValue = 0

		}

	}

	return

}

func rate(options structs.TSDBrateOptions, serie gorilla.Pnts) gorilla.Pnts {

	if len(serie) == 1 {
		return serie
	}

	rateSerie := gorilla.Pnts{}

	for i := 1; i < len(serie); i++ {

		if serie[i].Empty || serie[i-1].Empty {
			p := &pb.Point{
				Date:  serie[i].Date,
				Empty: true,
			}
			rateSerie = append(rateSerie, p)
			continue
		}

		var value float32

		if options.Counter && serie[i].Value < serie[i-1].Value {
			value = (float32(*options.CounterMax) + serie[i].Value - serie[i-1].Value) / float32((serie[i].Date)-(serie[i-1].Date))
			if options.ResetValue != 0 && float32(options.ResetValue) <= value {
				value = 0
			}
		} else {
			value = (serie[i].Value - serie[i-1].Value) / float32((serie[i].Date)-(serie[i-1].Date))
		}

		p := &pb.Point{
			Value: value,
			Date:  serie[i].Date,
			Empty: false,
		}

		rateSerie = append(rateSerie, p)
	}

	return rateSerie
}

func downsample(options structs.DSoptions, keepEmpties bool, start, end int64, serie gorilla.Pnts) gorilla.Pnts {

	switch options.Unit {
	case "sec":
		// start is already in seconds
	case "min":
		start = time.Unix(start, 0).Truncate(time.Minute).Unix()
	case "hour":
		start = time.Unix(start, 0).Truncate(time.Hour).Unix()
	case "day":
		//start = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local).Unix()
		start = time.Unix(start, 0).Truncate(24 * time.Hour).Unix()
	case "week":
		base := time.Unix(start, 0).Truncate(24 * time.Hour)
		for base.Weekday() != time.Monday {
			base = base.AddDate(0, 0, -1)
		}
		start = base.Unix()
	case "month":
		t := time.Unix(start, 0)
		base := t.Truncate(24 * time.Hour)
		for base.Month() == t.Month() {
			base = base.AddDate(0, 0, -1)
		}
		start = base.AddDate(0, 0, 1).Unix()
	case "year":
		start = time.Date(time.Unix(start, 0).Year(), time.January, 1, 0, 0, 0, 0, time.Local).Unix()
	}

	groupDate := start

	endInterval := getEndInterval(start, options.Unit, options.Value)

	var groupedCount float32

	groupedPoint := &pb.Point{}

	groupedSerie := gorilla.Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		//Ajusting for missing points
		for point.Date >= endInterval {
			if keepEmpties {
				groupedPoint.Date = groupDate

				if options.Fill == "zero" {
					groupedPoint.Value = 0
				} else {
					groupedPoint.Empty = true
				}

				groupedSerie = append(groupedSerie, groupedPoint)

				groupedPoint = &pb.Point{}
			}

			groupDate = endInterval

			endInterval = getEndInterval(endInterval, options.Unit, options.Value)
		}

		groupedCount++

		switch options.Downsample {
		case "avg":
			groupedPoint.Value += point.Value
		case "sum":
			groupedPoint.Value += point.Value
		case "max":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value > groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "min":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value < groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "pnt":
			groupedPoint.Value = groupedCount
		}

		if i+1 == len(serie) || serie[i+1].Date >= endInterval {

			groupedPoint.Date = groupDate

			groupDate = endInterval

			if options.Downsample == "avg" {
				groupedPoint.Value = groupedPoint.Value / groupedCount
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			groupedCount = 0

			groupedPoint = &pb.Point{}

			if i+1 != len(serie) {
				endInterval = getEndInterval(endInterval, options.Unit, options.Value)
			}
		}

	}

	if keepEmpties {
		for i := endInterval; i < end; i = endInterval {

			groupedPoint := &pb.Point{
				Date: endInterval,
			}

			if options.Fill == "zero" {
				groupedPoint.Value = 0
			} else {
				groupedPoint.Empty = true
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			endInterval = getEndInterval(i, options.Unit, options.Value)
		}
	}

	return groupedSerie
}

func getEndInterval(start int64, unit string, value int) int64 {

	var end int64

	switch unit {
	case "sec":
		end = start + int64(value)
	case "min":
		end = start + int64(minute*value)
	case "hour":
		end = start + int64(hour*value)
	case "day":
		end = start + int64(day*value)
	case "week":
		end = start + int64(week*value)
	case "month":
		startDate := time.Unix(start, 0)
		base := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.Local)
		end = base.AddDate(0, value, 0).Unix()
	case "year":
		startDate := time.Unix(start, 0)
		base := time.Date(startDate.Year(), time.January, 1, 0, 0, 0, 0, time.Local)
		end = base.AddDate(value, 0, 0).Unix()
	}

	return end
}

func merge(mergeType string, keepEmpties bool, serie gorilla.Pnts) gorilla.Pnts {

	mergedSerie := gorilla.Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		var mergedPoint *pb.Point

		if i < len(serie)-1 {

			j := i + 1

			nextPoint := serie[j]

			var mergedCount, nullCount float32

			mergedPoint = point

			mergedCount++

			if point.Empty {
				nullCount++
			}

			for point.Date == nextPoint.Date {

				i++

				mergedCount++

				if !nextPoint.Empty {

					mergedPoint.Empty = false

					switch mergeType {
					case "avg":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "sum":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "max":
						if nextPoint.Value > mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "min":
						if nextPoint.Value < mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "pnt":
						mergedPoint.Value = mergedCount
					}
				} else {
					nullCount++
				}

				if j == len(serie)-1 {
					break
				}

				j++

				nextPoint = serie[j]
			}

			if mergedCount-nullCount > 1 {

				if mergeType == "avg" {
					mergedPoint.Value = mergedPoint.Value / mergedCount
				}

			} else if nullCount == mergedCount && keepEmpties {
				mergedPoint.Empty = true
			}

		} else {
			mergedPoint = point
		}

		mergedSerie = append(mergedSerie, mergedPoint)

	}

	return mergedSerie
}

func filterValues(oper structs.FilterValueOperation, serie gorilla.Pnts) gorilla.Pnts {

	filteredSerie := gorilla.Pnts{}

	switch oper.BoolOper {
	case "<":
		for _, pnt := range serie {
			if pnt.Value < oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">":
		for _, pnt := range serie {
			if pnt.Value > oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "==":
		for _, pnt := range serie {
			if pnt.Value == oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">=":
		for _, pnt := range serie {
			if pnt.Value >= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "<=":
		for _, pnt := range serie {
			if pnt.Value <= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	}

	return filteredSerie
}

func round(val float32, roundOn float32, places int) float32 {

	var round float64

	pow := math.Pow(10, float64(places))

	digit := pow * float64(val)

	_, div := math.Modf(digit)

	if float32(div) >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}

	return float32(round / pow)

}
