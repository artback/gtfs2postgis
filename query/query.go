package query

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/artback/gtfs2postgis/config"
	"github.com/artback/gtfs2postgis/reader"
	_ "github.com/lib/pq"
	"github.com/nleof/goyesql"
	"os"
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
	pass := os.Getenv("POSTGRES_PASSWORD")
	if pass == "" {
		pass = c.Password
	}
	host := os.Getenv("POSTGRESS_HOST")
	if host == "" {
		host = c.Host
	}
	port, _ := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	if port == 0 {
		port = c.Port
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = c.User
	}
	db := os.Getenv("POSTGRES_DB")
	if db == "" {
		db = c.Database
	}
	if len(pass) > 0 {
		passwordArg = "password=" + pass
	}
	var err error
	db_string := fmt.Sprintf("host=%s port=%d user=%s %s dbname=%s sslmode=disable",
		host, port, user, passwordArg, db)
	r.db, err = sql.Open(c.Driver, db_string)
	return err
}

func (r *Repository) populateTable(tableName, filePath string) (*string, error) {
	rows, err := reader.CSV(filePath)
	if err != nil {
		return nil, err
	}
	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(dropTable(tableName))
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = r.createTable(tx, tableName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	message, err := r.loadTable(tx, tableName, rows)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if tableName == "stops" {
		err = r.updateGeom(tx, tableName)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()

	return message, err
}

func (r *Repository) PopulateTable(tableName, filePath string) (*string, error) {
	return r.populateTable(tableName, filePath)
}

func (r *Repository) runQuery(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}
func CopyIn(table string) string {
	return "COPY tmp_table FROM STDIN"
}
func createTemptable(table_name string) string {
	return "CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM " + table_name + " WITH NO DATA"
}
func copyFromTempTable(table_name string) string {
	return "INSERT INTO " + table_name + " SELECT * FROM tmp_table"
}
func dropTable(table_name string) string {
	return "DROP TABLE " + table_name + " CASCADE"
}

func (r *Repository) runCopyIn(tx *sql.Tx, tableName string, header []string, rows [][]string) (*string, error) {
	_, err := tx.Exec(createTemptable(tableName))

	stmt, err := tx.Prepare(CopyIn(tableName))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, row := range rows {
		args := make([]interface{}, len(row))
		for i, arg := range row {
			args[i], err = convertColumnType(header[i], arg)
			if err != nil {
				return nil, err
			}
		}
		stmt.Exec(args...)
		if err != nil {
			return nil, err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return nil, err
	}

	inserted := fmt.Sprintf("%d rows inserted into table \"%s\"", len(rows), tableName)
	fmt.Println(inserted)
	_, err = tx.Exec(copyFromTempTable(tableName))
	if err != nil {
		return nil, err
	}
	return &inserted, stmt.Close()
}

func (r *Repository) createTable(tx *sql.Tx, tableName string) error {
	return r.runQuery(tx, queries[goyesql.Tag("create-table-"+tableName)])
}

func (r *Repository) loadTable(tx *sql.Tx, tableName string, rows [][]string) (*string, error) {
	if len(rows) < 1 {
		return nil, errors.New(fmt.Sprintf("load %s table: no records found in the file", tableName))
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
	case "departure_time", "arrival_time":
		parts := strings.Split(arg, ":")
		int_parts := []int{}
		for i, _ := range parts {
			val, err := strconv.Atoi(parts[i])
			if err != nil {
				panic(err)
			}
			int_parts = append(int_parts, val)
		}
		if int_parts[0] < 4 {
			int_parts[0] = int_parts[0] + 24
		}
		return fmt.Sprintf("%02d:%02d:%02d", int_parts[0], int_parts[1], int_parts[2]), nil
	default:
		return arg, nil
	}
}
