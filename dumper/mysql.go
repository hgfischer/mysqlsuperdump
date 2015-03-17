package dumper

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
)

type MySQL struct {
	DB        *sql.DB
	SelectMap map[string]map[string]string
	WhereMap  map[string]string
	FilterMap map[string]string
}

// Lock the table (read only)
func (d *MySQL) LockTableReading(table string) (sql.Result, error) {
	return d.DB.Exec(fmt.Sprintf("LOCK TABLES `%s` READ", table))
}

// Flush table to ensure that the all active index pages are written to disk
func (d *MySQL) FlushTable(table string) (sql.Result, error) {
	return d.DB.Exec(fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// Release the global read locks
func (d *MySQL) UnlockTables() (sql.Result, error) {
	return d.DB.Exec(fmt.Sprintf("UNLOCK TABLES"))
}

// Get list of existing tables in database
func (d *MySQL) GetTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = d.DB.Query("SHOW FULL TABLES"); err != nil {
		return
	}
	for rows.Next() {
		var tableName, tableType string
		if err = rows.Scan(&tableName, &tableType); err != nil {
			return
		}
		if tableType == "BASE TABLE" {
			tables = append(tables, tableName)
		}
	}
	return
}

// Dump the script to create the table
func (d *MySQL) DumpCreateTable(w io.Writer, table string) error {
	fmt.Fprintf(w, "\n--\n-- Structure for table `%s`\n--\n\n", table)
	fmt.Fprintf(w, "DROP TABLE IF EXISTS `%s`;\n", table)
	row := d.DB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", table))
	var tname, ddl string
	if err := row.Scan(&tname, &ddl); err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", ddl)
	return nil
}

// Get the column list for the SELECT, applying the select map from config file.
func (d *MySQL) GetColumnsForSelect(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = d.DB.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	if columns, err = rows.Columns(); err != nil {
		return
	}
	for k, column := range columns {
		replacement, ok := d.SelectMap[table][column]
		if ok {
			columns[k] = fmt.Sprintf("%s AS `%s`", replacement, column)
		} else {
			columns[k] = fmt.Sprintf("`%s`", column)
		}
	}
	return
}

// Get the complete SELECT query to fetch data from database
func (d *MySQL) GetSelectQueryFor(table string) (query string, err error) {
	cols, err := d.GetColumnsForSelect(table)
	if err != nil {
		return "", err
	}
	query = fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(cols, ", "), table)
	if where, ok := d.WhereMap[table]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

// Get the number of rows the select will return
func (d *MySQL) GetRowCount(table string) (count uint64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if where, ok := d.WhereMap[table]; ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	row := d.DB.QueryRow(query)
	if err = row.Scan(&count); err != nil {
		return
	}
	return
}

// Dump comments including table name and row count to w
func (d *MySQL) DumpTableHeader(w io.Writer, table string) (count uint64, err error) {
	fmt.Fprintf(w, "\n--\n-- Data for table `%s`", table)
	if count, err = d.GetRowCount(table); err != nil {
		return
	}
	fmt.Fprintf(w, " -- %d rows\n--\n\n", count)
	return
}

// Write the query to lock writes in the specified table
func (d *MySQL) DumpTableLockWrite(w io.Writer, table string) {
	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
}

// Write the query to unlock tables
func (d *MySQL) DumpUnlockTables(w io.Writer) {
	fmt.Fprintln(w, "UNLOCK TABLES;")
}

func (d *MySQL) selectAllDataFor(table string) (rows *sql.Rows, columns []string, err error) {
	var selectQuery string
	if selectQuery, err = d.GetSelectQueryFor(table); err != nil {
		return
	}
	if rows, err = d.DB.Query(selectQuery); err != nil {
		return
	}
	if columns, err = rows.Columns(); err != nil {
		return
	}
	return
}

// Get the table data
func (d *MySQL) DumpTableData(w io.Writer, table string) (err error) {
	rows, columns, err := d.selectAllDataFor(table)
	if err != nil {
		return
	}

	values := make([]*sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	query := fmt.Sprintf("INSERT INTO `%s` VALUES", table)
	data := make([]string, 0)
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			return err
		}
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

	return
}
