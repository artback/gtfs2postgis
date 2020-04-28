package query

import (
	"database/sql"
	"github.com/artback/gtfs2postgis/time"
	"github.com/nleof/goyesql"
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

func (r *Repository) updateGeom(tx *sql.Tx, tableName string) error {
	return r.runQuery(tx, queries[goyesql.Tag("update-geom-"+tableName)])
}
