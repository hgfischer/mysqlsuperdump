package dumper

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
)

const (
	ExtendedInsertDefaultRowCount = 100
)

type mySQL struct {
	DB                 *sql.DB
	SelectMap          map[string]map[string]string
	WhereMap           map[string]string
	FilterMap          map[string]string
	UseTableLock       bool
	Log                *log.Logger
	ExtendedInsertRows int
}

func NewMySQLDumper(db *sql.DB, logger *log.Logger) *mySQL {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	return &mySQL{DB: db, Log: logger, ExtendedInsertRows: ExtendedInsertDefaultRowCount}
}

// Lock the table (read only)
func (d *mySQL) LockTableReading(table string) (sql.Result, error) {
	d.Log.Println("Locking table", table, "for reading")
	return d.DB.Exec(fmt.Sprintf("LOCK TABLES `%s` READ", table))
}

// Flush table to ensure that the all active index pages are written to disk
func (d *mySQL) FlushTable(table string) (sql.Result, error) {
	d.Log.Println("Flushing table", table)
	return d.DB.Exec(fmt.Sprintf("FLUSH TABLES `%s`", table))
}

// Release the global read locks
func (d *mySQL) UnlockTables() (sql.Result, error) {
	d.Log.Println("Unlocking tables")
	return d.DB.Exec(fmt.Sprintf("UNLOCK TABLES"))
}

// Get list of existing tables in database
func (d *mySQL) GetTables() (tables []string, err error) {
	tables = make([]string, 0)
	var rows *sql.Rows
	if rows, err = d.DB.Query("SHOW FULL TABLES"); err != nil {
		return
	}
	defer rows.Close()
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
func (d *mySQL) DumpCreateTable(w io.Writer, table string) error {
	d.Log.Println("Dumping structure for table", table)
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
func (d *mySQL) GetColumnsForSelect(table string) (columns []string, err error) {
	var rows *sql.Rows
	if rows, err = d.DB.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table)); err != nil {
		return
	}
	defer rows.Close()
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
func (d *mySQL) GetSelectQueryFor(table string) (query string, err error) {
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
func (d *mySQL) GetRowCount(table string) (count uint64, err error) {
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
func (d *mySQL) DumpTableHeader(w io.Writer, table string) (count uint64, err error) {
	fmt.Fprintf(w, "\n--\n-- Data for table `%s`", table)
	if count, err = d.GetRowCount(table); err != nil {
		return
	}
	fmt.Fprintf(w, " -- %d rows\n--\n\n", count)
	return
}

// Write the query to lock writes in the specified table
func (d *mySQL) DumpTableLockWrite(w io.Writer, table string) {
	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
}

// Write the query to unlock tables
func (d *mySQL) DumpUnlockTables(w io.Writer) {
	fmt.Fprintln(w, "UNLOCK TABLES;")
}

func (d *mySQL) selectAllDataFor(table string) (rows *sql.Rows, columns []string, err error) {
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
func (d *mySQL) DumpTableData(w io.Writer, table string) (err error) {
	d.Log.Println("Dumping data for table", table)
	rows, columns, err := d.selectAllDataFor(table)
	if err != nil {
		return
	}
	defer rows.Close()

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
		if len(data) >= d.ExtendedInsertRows {
			fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
			data = make([]string, 0)
		}
	}

	if len(data) > 0 {
		fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(data, ",\n"))
	}

	return
}

func (d *mySQL) Dump(w io.Writer) (err error) {
	fmt.Fprintf(w, "SET NAMES utf8;\n")
	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")

	d.Log.Println("Getting table list...")
	tables, err := d.GetTables()
	if err != nil {
		return
	}

	for _, table := range tables {
		if d.FilterMap[table] != "ignore" {
			skipData := d.FilterMap[table] == "nodata"
			if !skipData && d.UseTableLock {
				d.LockTableReading(table)
				d.FlushTable(table)
			}
			d.DumpCreateTable(w, table)
			if !skipData {
				cnt, err := d.DumpTableHeader(w, table)
				if err != nil {
					return err
				}
				if cnt > 0 {
					d.DumpTableLockWrite(w, table)
					d.DumpTableData(w, table)
					fmt.Fprintln(w)
					d.DumpUnlockTables(w)
					if d.UseTableLock {
						d.UnlockTables()
					}
				}
			}
		}
	}

	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")
	return
}
