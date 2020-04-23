package query

func CopyIn() string {
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
