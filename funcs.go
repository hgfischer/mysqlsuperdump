package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"strings"
)

func Query(db *sql.DB, q string) (*sql.Rows, error) {
	debug.Printf("Query: %s\n", q)
	return db.Query(q)
}

func QueryRow(db *sql.DB, q string) *sql.Row {
	debug.Printf("QueryRow: %s\n", q)
	return db.QueryRow(q)
}

func ExecQuery(db *sql.DB, q string) (sql.Result, error) {
	debug.Printf("ExecQuery: %s\n", q)
	return db.Exec(q)
}

// Check if err is not nil. If it's not, prints error and exit program
func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

// Lock the table (read only)
func lockTable(db *sql.DB, table string) (sql.Result, error) {
	return ExecQuery(db, fmt.Sprintf("LOCK TABLES `%s` READ", table))
}

// Flush table to ensure that the all active index pages are written to disk
func flushTable(db *sql.DB, table string) (sql.Result, error) {
	return ExecQuery(db, fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// Release the global read locks
func unlockTables(db *sql.DB) (sql.Result, error) {
	return ExecQuery(db, fmt.Sprintf("UNLOCK TABLES"))
}

// Get list of existing tables in database
func getTables(db *sql.DB) (tables []string) {
	tables = make([]string, 0)
	rows, err := Query(db, "SHOW FULL TABLES")
	checkError(err)
	for rows.Next() {
		var tableName string
		var tableType string
		err = rows.Scan(&tableName, &tableType)
		checkError(err)
		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
		// TODO feature to export views as well
	}
	checkError(rows.Err())
	return
}

// Dump the script to create the table
func dumpCreateTable(w io.Writer, db *sql.DB, table string) {
	fmt.Fprintf(w, "\n--\n")
	fmt.Fprintf(w, "-- Structure for table `%s`\n", table)
	fmt.Fprintf(w, "--\n\n")
	fmt.Fprintf(w, "DROP TABLE IF EXISTS `%s`;\n", table)
	row := QueryRow(db, fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tname, ddl string
	err := row.Scan(&tname, &ddl)
	checkError(err)
	fmt.Fprintf(w, "%s;\n", ddl)
}

// Get the column list for the SELECT, applying the select map
// from config file.
func getColumnListForSelect(db *sql.DB, table string) string {
	rows, err := Query(db, fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table))
	checkError(err)
	columns, err := rows.Columns()
	checkError(err)
	for k, column := range columns {
		replacement, ok := selectMap[table][column]
		if ok {
			columns[k] = fmt.Sprintf("%s AS `%s`", replacement, column)
		} else {
			columns[k] = fmt.Sprintf("`%s`", column)
		}
	}
	return strings.Join(columns, ", ")
}

// Get the complete SELECT query to fetch data from database
func getSelectQueryFor(db *sql.DB, table string) (query string) {
	columns := getColumnListForSelect(db, table)
	query = fmt.Sprintf("SELECT %s FROM `%s`", columns, table)
	where, ok := whereMap[table]
	if ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

// Get the number of rows the select will return
func getSelectCountQueryFor(db *sql.DB, table string) (query string) {
	query = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	where, ok := whereMap[table]
	if ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

// Get the table data
func dumpTableData(w io.Writer, db *sql.DB, table string) {
	fmt.Fprintf(w, "\n--\n-- Data for table `%s`", table)

	var count uint64
	row := QueryRow(db, getSelectCountQueryFor(db, table))
	err := row.Scan(&count)
	checkError(err)
	fmt.Fprintf(w, " -- %d rows\n--\n\n", count)

	if count == 0 {
		return // Avoid table lock if empty
	}

	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
	query := fmt.Sprintf("INSERT INTO `%s` VALUES", table)
	data := make([]string, 0)

	selectQuery := getSelectQueryFor(db, table)
	rows, err := Query(db, selectQuery)
	checkError(err)
	columns, err := rows.Columns()
	checkError(err)

	values := make([]*sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		checkError(err)

		vals := make([]string, 0)
		for _, col := range values {
			val := "NULL"
			if col != nil {
				val = fmt.Sprintf("'%s'", escape(string(*col)))
			}
			vals = append(vals, val)
		}

		data = append(data, fmt.Sprintf("( %s )", strings.Join(vals, ", ")))
		if len(data) >= 100 {
			fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
			data = make([]string, 0)
		}
	}

	if len(data) > 0 {
		fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
	}

	fmt.Fprintf(w, "\nUNLOCK TABLES;\n")
}

func escape(str string) string {
	var esc string
	var buf bytes.Buffer
	last := 0
	for i, c := range str {
		switch c {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, str[last:i])
		io.WriteString(&buf, esc)
		last = i + 1
	}
	io.WriteString(&buf, str[last:])
	return buf.String()
}
