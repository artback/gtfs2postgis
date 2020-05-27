package query

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/allbin/gtfs2postgis/config"
	"github.com/allbin/gtfs2postgis/reader"
	_ "github.com/lib/pq"
	"github.com/nleof/goyesql"
	"strings"
)

const sqlFile = "./query/queries.sql"

type Repository struct {
	db      *sql.DB
	queries goyesql.Queries
}

func (r *Repository) Connect(c config.DatabaseConfiguration) error {
	r.queries = goyesql.MustParseFile(sqlFile)
	passwordArg := ""
	if len(c.Password) > 0 {
		passwordArg = "password=" + c.Password
	}
	var err error
	dbString := fmt.Sprintf("host=%s port=%d user=%s %s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, passwordArg, c.Database)
	r.db, err = sql.Open(c.Driver, dbString)
	if err != nil {
		return err
	}
	return r.db.Ping()
}
func (r *Repository) CreatePostgis() (sql.Result, error) {
	return r.db.Exec(r.queries[goyesql.Tag("create-postgis")])
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
	if _, err := dropTable(tx, tableName); err != nil {
		return nil, err
	}
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

func (r *Repository) PopulateTable(filePath string) (*string, error) {
	s := strings.Split(filePath, "/")
	tableName := strings.Split(s[len(s)-1], ".")[0]
	return r.populateTable(tableName, filePath)
}

func (r *Repository) runQuery(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}

func (r *Repository) runCopyIn(tx *sql.Tx, tableName string, header []string, rows [][]string) (*string, error) {
	createTemptable(tx, tableName)
	stmt, err := copyIn(tx)
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
		if _, err := stmt.Exec(args...); err != nil {
			return nil, err
		}
	}
	if _, err = stmt.Exec(); err != nil {
		return nil, err
	}
	if _, err = copyFromTempTable(tx, tableName); err != nil {
		return nil, err
	}
	inserted := fmt.Sprintf("%d rows inserted into table \"%s\"", len(rows), tableName)
	return &inserted, stmt.Close()
}

func (r *Repository) createTable(tx *sql.Tx, tableName string) error {
	return r.runQuery(tx, r.queries[goyesql.Tag("create-table-"+tableName)])
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
	return r.runQuery(tx, r.queries[goyesql.Tag("update-geom-"+tableName)])
}
