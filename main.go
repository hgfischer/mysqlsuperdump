package main

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hgfischer/mysqlsuperdump/dumper"
)

func main() {
	checkError := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	cfg := newConfig()
	checkError(cfg.parseAll())
	verbosely := cfg.getVerboseLogger()

	verbosely.Println("Connecting to MySQL database at", cfg.dsn)
	db, err := sql.Open("mysql", cfg.dsn)
	checkError(err)
	defer db.Close()

	dumpr := &dumper.MySQL{
		DB:           db,
		SelectMap:    cfg.selectMap,
		WhereMap:     cfg.whereMap,
		FilterMap:    cfg.filterMap,
		UseTableLock: cfg.useTableLock,
		Log:          verbosely,
	}

	w, err := cfg.initOutput()
	checkError(err)
	defer w.Close()

	verbosely.Println("Starting dump")
	checkError(dumpr.Dump(w))
}
