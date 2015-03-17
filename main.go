package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/hgfischer/mysql"
)

func main() {
	checkError := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	cfg := newConfig()
	checkError(cfg.parseAll())

	dumper := &mysqlDumper{cfg}

	verbose := cfg.verbose
	verbose.Printf("> Using table locks: %t\n", cfg.useTableLock)

	verbose.Printf("> Connecting to MySQL database at %s\n", cfg.dsn)
	db, err := sql.Open("mysql", cfg.dsn)
	checkError(err)
	defer db.Close()

	w, err := cfg.initOutput()
	checkError(err)

	fmt.Fprintf(w, "SET NAMES utf8;\n")
	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")

	verbose.Printf("> Getting table list...\n")
	tables := getTables(db)

	for _, table := range tables {
		if cfg.filterMap[table] != "ignore" {
			skipData := cfg.filterMap[table] == "nodata"
			if !skipData && cfg.useTableLock {
				verbose.Printf("> Locking table %s...\n", table)
				lockTable(db, table)
				flushTable(db, table)
			}
			verbose.Printf("> Dumping structure for table %s...\n", table)
			dumpCreateTable(w, db, table)
			if !skipData {
				verbose.Printf("> Dumping data for table %s...\n", table)
				dumpTableData(w, db, table)
				if cfg.useTableLock {
					verbose.Printf("> Unlocking table %s...\n", table)
					unlockTables(db)
				}
			}
		}
	}

	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 1;\n")
}
