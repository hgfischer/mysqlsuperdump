package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	conf "github.com/dlintw/goconf"
)

var (
	configFile         string
	dsn                string
	extendedInsertRows int
	whereMap           = make(map[string]string, 0)
	selectMap          = make(map[string]map[string]string, 0)
	filterMap          = make(map[string]string, 0)
	output             = flag.String("o", "", "Output path. Default is stdout")
	verboseFlag        = flag.Bool("v", false, "Enable printing status information")
	debugFlag          = flag.Bool("d", false, "Enable printing of debug information")
	verbose            Bool
	debug              Bool
	useTableLock       bool
)

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
	verbose = Bool(*verboseFlag)
	debug = Bool(*debugFlag)
	return
}

// Read config file, inclusing wheres and selects maps
func readConfigFile() {
	cfg, err := conf.ReadConfigFile(configFile)
	checkError(err)
	dsn, err = cfg.GetString("mysql", "dsn")
	checkError(err)
	extendedInsertRows, err = cfg.GetInt("mysql", "extended_insert_rows")
	checkError(err)
	useTableLock, err = cfg.GetBool("mysql", "use_table_lock") // return false on error

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

	filters, err := cfg.GetOptions("filter")
	checkError(err)
	for _, table := range filters {
		filterMap[table], err = cfg.GetString("filter", table)
		checkError(err)
	}
}
