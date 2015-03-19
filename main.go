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
	db.SetMaxOpenConns(cfg.maxOpenConns)
	checkError(err)
	defer db.Close()

	dumpr := dumper.NewMySQLDumper(db, verbosely)
	dumpr.SelectMap = cfg.selectMap
	dumpr.WhereMap = cfg.whereMap
	dumpr.FilterMap = cfg.filterMap
	dumpr.UseTableLock = cfg.useTableLock
	dumpr.ExtendedInsertRows = cfg.extendedInsRows

	w, err := cfg.initOutput()
	checkError(err)
	defer w.Close()

	verbosely.Println("Starting dump")
	checkError(dumpr.Dump(w))
}
