package query

import (
	"github.com/allbin/gtfs2postgis/time"
	"strconv"
	"strings"
)

func convertColumnType(column, arg string) (interface{}, error) {
	if len(arg) == 0 {
		return nil, nil
	}
	arg = strings.TrimSpace(arg)
	switch column {
	case "stop_lat", "stop_lon":
		return strconv.ParseFloat(arg, 8)
	case "departure_time", "arrival_time":
		return time.AddHoursToTimeString(arg, ":", 24), nil
	default:
		return arg, nil
	}
}
