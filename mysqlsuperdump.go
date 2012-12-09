package main

import (
	"flag"
	"fmt"
	"os"
	"io"
	"strings"
	"github.com/hgfischer/goconf"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
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
)

// MAIN
func main() {
	var err error
	var w io.Writer

	parseCommandLine()
	readConfigFile()

	raddr := fmt.Sprintf("%s:%d", hostname, port)
	db := mysql.New("tcp", "", raddr, username, password, database)
	err = db.Connect()
	checkError(err)

	if *output == "" {
		w = os.Stdout
	} else {
		w, err = os.Create(*output)
		checkError(err)
	}

	fmt.Fprintf(w, "SET NAMES 'utf8';\n")
	fmt.Fprintf(w, "SET FOREIGN_KEY_CHECKS = 0;\n")

	tables := getTables(db)
	for _, table := range tables {
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

func readConfigFile() {
	cfg, err := conf.ReadConfigFile(configFile)
	checkError(err)
	hostname, err = cfg.GetString("mysql", "hostname")
	checkError(err)
	port, err = cfg.GetInt("mysql", "port")
	checkError(err);
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
		if (selectMap[table] == nil) {
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

func dumpCreateTable(w io.Writer, db mysql.Conn, table string) {
	fmt.Fprintf(w, "\n--\n")
	fmt.Fprintf(w, "-- Table structure for table `%s`\n", table)
	fmt.Fprintf(w, "--\n\n")
	fmt.Fprintf(w, "DROP TABLE IF EXISTS `%s`;\n", table)
	row, _, err := db.QueryFirst("SHOW CREATE TABLE `%s`", table)
	checkError(err)
	fmt.Fprintf(w, "%s;\n", row.Str(1))
}

func getColumnListForSelect(db mysql.Conn, table string) (string) {
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

func getSelectQueryFor(db mysql.Conn, table string) (query string) {
	columns := getColumnListForSelect(db, table)
	query = fmt.Sprintf("SELECT %s FROM `%s`", columns, table)
	where, ok := whereMap[table]
	if ok {
		query = fmt.Sprintf("%s WHERE %s", query, where)
	}
	return
}

func dumpTableData(w io.Writer, db mysql.Conn, table string) {
	fmt.Fprintf(w, "\n--\n")
	fmt.Fprintf(w, "-- Dumping data for table `%s`\n", table)
	fmt.Fprintf(w, "--\n\n")
	fmt.Fprintf(w, "LOCK TABLES `%s` WRITE;\n", table)
	query := fmt.Sprintf("INSERT INTO `%s` SET ", table)
	res, err := db.Start(getSelectQueryFor(db, table))
	checkError(err)
	row := res.MakeRow()
	for {
		err = res.ScanRow(row)
		if err == io.EOF {
			break
		}
		checkError(err)
		sets := make([]string, 0)
		for k, _ := range row {
			sets = append(sets, fmt.Sprintf("`%s` = '%s'", res.Fields()[k].Name, row.Str(k)))
		}
		fmt.Fprintf(w, "%s %s;\n", query, strings.Join(sets, ", "))
	}
	fmt.Fprintf(w, "\nUNLOCK TABLES;\n", table)
}
