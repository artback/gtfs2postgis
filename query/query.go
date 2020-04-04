package query

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/artback/gtfs2postgis/config"
	"github.com/artback/gtfs2postgis/reader"
	_ "github.com/lib/pq"
	"github.com/nleof/goyesql"
	"strconv"
	"strings"
)

type Repository struct{ db *sql.DB }

var queries goyesql.Queries

func init() {
	queries = goyesql.MustParseFile("./query/queries.sql")
}

func (r *Repository) Connect(c config.DatabaseConfiguration) error {
	passwordArg := ""
	if len(c.Password) > 0 {
		passwordArg = "password=" + c.Password
	}

	var err error
	r.db, err = sql.Open(c.Driver, fmt.Sprintf("host=%s port=%d user=%s %s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, passwordArg, c.Database))

	return err
}

func (r *Repository) populateTable(tableName, filePath string) error {
	rows, err := reader.CSV(filePath)
	if err != nil {
		return err
	}
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	err = r.createTable(tx, tableName)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = r.loadTable(tx, tableName, rows)
	if err != nil {
		tx.Rollback()
		return err
	}

	if tableName == "stops" {
		err = r.updateGeom(tx, tableName)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()

	return err
}

func (r *Repository) PopulateTable(tableName, filePath string) error {
	return r.populateTable(tableName, filePath)
}

func (r *Repository) runQuery(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}
func CopyIn(table string) string {
	return "COPY tmp_table FROM STDIN"
}
func createTemptable(table string) string {
	return "CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM " + table + " WITH NO DATA"
}
func alterTempForStop() string {
	return "ALTER TABLE tmp_table DROP COLUMN geom"
}
func copyFromTempTable(table string, pk string) string {
	return "INSERT INTO " + table + " SELECT DISTINCT ON (" + pk + ") * FROM tmp_table ORDER BY (" + pk + ") ON CONFLICT DO NOTHING"
}

func (r *Repository) runCopyIn(tx *sql.Tx, tableName string, header []string, rows [][]string) error {
	_, err := tx.Exec(createTemptable(tableName))
	if tableName == "stops" {
		tx.Exec(queries[goyesql.Tag("drop-geom")])
	}
	stmt, err := tx.Prepare(CopyIn(tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range rows {
		args := make([]interface{}, len(row))
		for i, arg := range row {
			args[i], err = convertColumnType(header[i], arg)
			if err != nil {
				return err
			}
		}
		stmt.Exec(args...)
		if err != nil {
			return err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("%d rows inserted into table \"%s\"", len(rows), tableName))
	_, err = tx.Exec(copyFromTempTable(tableName, header[0]))
	if err != nil {
		return err
	}
	return stmt.Close()
}

func (r *Repository) createTable(tx *sql.Tx, tableName string) error {
	return r.runQuery(tx, queries[goyesql.Tag("create-table-"+tableName)])
}

func (r *Repository) loadTable(tx *sql.Tx, tableName string, rows [][]string) error {
	if len(rows) < 1 {
		return errors.New(fmt.Sprintf("load %s table: no records found in the file", tableName))
	}

	header := rows[0]
	header[0] = strings.TrimPrefix(header[0], "\uFEFF")

	return r.runCopyIn(tx, tableName, header, rows[1:])
}

func (r *Repository) updateGeom(tx *sql.Tx, tableName string) error {
	return r.runQuery(tx, queries[goyesql.Tag("update-geom-"+tableName)])
}

func convertColumnType(column, arg string) (interface{}, error) {
	if len(arg) == 0 {
		return nil, nil
	}
	arg = strings.TrimSpace(arg)
	switch column {
	case "stop_lat", "stop_lon":
		return strconv.ParseFloat(arg, 8)
	default:
		return arg, nil
	}
}
