package query

import (
	"database/sql"
	"fmt"
)

func copyIn(tx *sql.Tx) (*sql.Stmt, error) {
	return tx.Prepare("COPY tmp_table FROM STDIN")
}
func createTemptable(tx *sql.Tx, tableName string) (sql.Result, error) {
	return tx.Exec(fmt.Sprintf("CREATE TEMP TABLE tmp_table ON COMMIT DROP AS SELECT * FROM %s WITH NO DATA", tableName))
}
func copyFromTempTable(tx *sql.Tx, tableName string) (sql.Result, error) {
	return tx.Exec(fmt.Sprintf("INSERT INTO %s SELECT * FROM tmp_table", tableName))
}
func dropTable(tx *sql.Tx, tableName string) (sql.Result, error) {
	return tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
}
