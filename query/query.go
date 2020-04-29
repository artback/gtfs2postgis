package query

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/allbin/gtfs2postgis/config"
	"github.com/allbin/gtfs2postgis/reader"
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
	host := os.Getenv("POSTGRES_HOST")
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
	if err != nil {
		return err
	}
	return r.db.Ping()
}
func (r *Repository) CreatePostgis() (sql.Result, error) {
	return r.db.Exec(queries[goyesql.Tag("create-postgis")])
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

func (r *Repository) PopulateTable(tableName, filePath string) string {
	m, err := r.populateTable(tableName, filePath)
	if err != nil {
		panic(err)
	}
	if m != nil {
		return fmt.Sprintln(*m)
	}
	return ""
}

func (r *Repository) runQuery(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}

func (r *Repository) runCopyIn(tx *sql.Tx, tableName string, header []string, rows [][]string) (*string, error) {
	_, err := tx.Exec(createTemptable(tableName))

	stmt, err := tx.Prepare(CopyIn())
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
