// Copyright 2012 Herbert G. Fischer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// mysqlsuperdump is a program that generates MySQL partial and secure dumps
// With it you can specify the WHERE clause for each table being dumped and
// also value replacements for each table.column.
package main

import (
	"flag"
	"fmt"
	"github.com/hgfischer/goconf"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"io"
	"os"
	"strings"
)

var (
	configFile string
	hostname   = "localhost"
	port       = 3306
	username   = "root"
	password   string
	database   = "mysql"
	whereMap   = make(map[string]string, 0)
	selectMap  = make(map[string]map[string]string, 0)
	output     = flag.String("o", "", "Output path. Default is stdout")
	verbose    = Verbose(*flag.Bool("v", false, "Enable verbosity"))
)

type Verbose bool

func (this Verbose) Printf(s string, a ...interface{}) {
	if this {
		fmt.Printf(s, a...)
	}
}

// MAIN
func main() {
	var err error
	var w io.Writer

	parseCommandLine()
	readConfigFile()

	raddr := fmt.Sprintf("%s:%d", hostname, port)
	db := mysql.New("tcp", "", raddr, username, password, database)
	db.Register("SET NAMES utf8")
	verbose.Printf("Connecting to MySQL database %s at %s@%s\n", database, username, raddr)
	err = db.Connect()
	checkError(err)

	if *output == "" {
		w = os.Stdout
	} else {
		w, err = os.Create(*output)
		checkError(err)
	}

	fmt.Fprintf(w, "SET NAMES utf8;\n")
	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")

	verbose.Printf("Getting table list...\n")
	tables := getTables(db)
	for _, table := range tables {
		verbose.Printf("Dumping structure and data for table %s.%s...\n", database, table)
		dumpCreateTable(w, db, table)
		dumpTableData(w, db, table)
	}

	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")
}

// Check if err is not nil. If it's not, prints error and exit program
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(-1)
	}
}

// Print command line help and exit application
func printUsage() {
	fmt.Fprintf(os.Stderr,
		"Usage: mysqlsuperdump [flags] [path to config file]\n")
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
	os.Exit(1)
}

// Parse command line options and parameters
func parseCommandLine() {
	flag.Usage = printUsage
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Error: Missing parameters\n")
		flag.Usage()
	}
	configFile = flag.Arg(0)
	return
}

// Read config file, inclusing wheres and selects maps
func readConfigFile() {
	cfg, err := conf.ReadConfigFile(configFile)
	checkError(err)
	hostname, err = cfg.GetString("mysql", "hostname")
	checkError(err)
	port, err = cfg.GetInt("mysql", "port")
	checkError(err)
	username, err = cfg.GetString("mysql", "username")
	checkError(err)
	password, err = cfg.GetString("mysql", "password")
	checkError(err)
	database, err = cfg.GetString("mysql", "database")
	checkError(err)

	selects, err := cfg.GetOptions("select")
	checkError(err)
	for _, tablecol := range selects {
		split := strings.Split(tablecol, ".")
		table := split[0]
		column := split[1]
		if selectMap[table] == nil {
			selectMap[table] = make(map[string]string, 0)
		}
		selectMap[table][column], err = cfg.GetString("select", tablecol)
		checkError(err)
	}

	wheres, err := cfg.GetOptions("where")
	checkError(err)
	for _, table := range wheres {
		whereMap[table], err = cfg.GetString("where", table)
		checkError(err)
	}
}

// Get list of existing tables in database
func getTables(db mysql.Conn) (tables []string) {
	tables = make([]string, 0)
	rows, _, err := db.Query("SHOW TABLES")
	checkError(err)
	for _, row := range rows {
		for k, _ := range row {
			tables = append(tables, row.Str(k))
		}
	}
	return
}

// Dump the script to create the table
func dumpCreateTable(w io.Writer, db mysql.Conn, table string) {
	fmt.Fprintf(w, "\n--\n")
	fmt.Fprintf(w, "-- Table structure for table `%s`\n", table)
	fmt.Fprintf(w, "--\n\n")
	fmt.Fprintf(w, "DROP TABLE IF EXISTS `%s`;\n", table)
	row, _, err := db.QueryFirst("SHOW CREATE TABLE `%s`", table)
	checkError(err)
	fmt.Fprintf(w, "%s;\n", row.Str(1))
}

// Get the column list for the SELECT, applying the select map
// from config file.
func getColumnListForSelect(db mysql.Conn, table string) string {
	columns := make([]string, 0)
	rows, res, err := db.Query("SHOW COLUMNS FROM `%s`", table)
	checkError(err)
	for _, row := range rows {
		column := row.Str(res.Map("Field"))
		replacement, ok := selectMap[table][column]
		if ok {
			column = fmt.Sprintf("%s AS `%s`", replacement, column)
		}
		columns = append(columns, column)
	}
	return strings.Join(columns, ", ")
}

// Get the complete SELECT query to fetch data from database
func getSelectQueryFor(db mysql.Conn, table string) (query string) {
	columns := getColumnListForSelect(db, table)
	query = fmt.Sprintf("SELECT %s FROM `%s`", columns, table)
	where, ok := whereMap[table]
	if ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

// Get the number of rows the select will return
func getSelectCountQueryFor(db mysql.Conn, table string) (query string) {
	query = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	where, ok := whereMap[table]
	if ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

// Get the table data
func dumpTableData(w io.Writer, db mysql.Conn, table string) {
	fmt.Fprintf(w, "\n--\n-- Dumping data for table `%s`\n--\n\n", table)

	rowCnt, _, err := db.QueryFirst(getSelectCountQueryFor(db, table))
	checkError(err)
	if rowCnt.Int(0) == 0 {
		fmt.Fprintf(w, "--\n-- Empty table\n--\n\n")
		return
	} else {
		fmt.Fprintf(w, "--\n-- %d rows\n--\n\n", rowCnt.Int(0))
	}

	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
	query := fmt.Sprintf("INSERT INTO `%s` VALUES", table)
	rows := make([]string, 0)

	res, err := db.Start(getSelectQueryFor(db, table))
	checkError(err)
	row := res.MakeRow()

	for {
		err = res.ScanRow(row)
		if err == io.EOF {
			break
		}
		checkError(err)

		vals := make([]string, 0)
		for k, col := range row {
			val := "NULL"
			if col != nil {
				val = fmt.Sprintf("'%s'", db.EscapeString(row.Str(k)))
			}
			vals = append(vals, val)
		}

		rows = append(rows, fmt.Sprintf("( %s )", strings.Join(vals, ", ")))
		if len(rows) >= 00 {
			fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(rows, ",\n"))
			rows = make([]string, 0)
		}
	}

	if len(rows) > 0 {
		fmt.Fprintf(w, "%s\n%s;\n", query, strings.Join(rows, ",\n"))
	}

	fmt.Fprintf(w, "\nUNLOCK TABLES;\n")
}
