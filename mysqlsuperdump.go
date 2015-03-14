package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"

	_ "github.com/hgfischer/mysql"
)

// MAIN
func main() {
	var err error
	var w io.Writer

	parseCommandLine()
	readConfigFile()

	verbose.Printf("> Using table locks: %t\n", useTableLock)

	verbose.Printf("> Connecting to MySQL database at %s\n", dsn)
	db, err := sql.Open("mysql", dsn)
	checkError(err)
	defer db.Close()

	if *output == "" {
		w = os.Stdout
	} else {
		w, err = os.Create(*output)
		checkError(err)
	}

	fmt.Fprintf(w, "SET NAMES utf8;\n")
	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")

	verbose.Printf("> Getting table list...\n")
	tables := getTables(db)

	for _, table := range tables {
		if filterMap[table] != "ignore" {
			skipData := filterMap[table] == "nodata"
			if !skipData && useTableLock {
				verbose.Printf("> Locking table %s...\n", table)
				lockTable(db, table)
				flushTable(db, table)
			}
			verbose.Printf("> Dumping structure for table %s...\n", table)
			dumpCreateTable(w, db, table)
			if !skipData {
				verbose.Printf("> Dumping data for table %s...\n", table)
				dumpTableData(w, db, table)
				if useTableLock {
					verbose.Printf("> Unlocking table %s...\n", table)
					unlockTables(db)
				}
			}
		}
	}

	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")
}
