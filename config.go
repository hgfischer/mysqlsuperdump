package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	ini "github.com/dlintw/goconf"
)

const UseStdout = "-"

type config struct {
	dsn             string
	maxOpenConns    int
	output          string
	file            string
	verbose         bool
	selectMap       map[string]map[string]string
	whereMap        map[string]string
	filterMap       map[string]string
	useTableLock    bool
	extendedInsRows int
	cfg             *ini.ConfigFile
}

func newConfig() *config {
	return &config{
		whereMap:  make(map[string]string, 0),
		selectMap: make(map[string]map[string]string, 0),
		filterMap: make(map[string]string, 0),
	}
}

func (c *config) usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] [config file]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
	os.Exit(1)
}

func (c *config) parseAll() (err error) {
	if err = c.parseCommandLine(); err != nil {
		return
	}
	if err = c.parseConfigFile(); err != nil {
		return
	}
	return
}

func (c *config) getVerboseLogger() *log.Logger {
	w := ioutil.Discard
	if c.verbose {
		w = os.Stdout
	}
	return log.New(w, "mysqlsuperdump: ", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
}

func (c *config) parseCommandLine() (err error) {
	flag.Usage = c.usage
	flag.StringVar(&(c.output), "o", UseStdout, "Output path. Default is stdout")
	flag.BoolVar(&(c.verbose), "v", false, "Enable printing status information")
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return errors.New("Missing parameters")
	}
	c.file = flag.Arg(0)
	return
}

func (c *config) parseConfigFile() (err error) {
	if c.cfg, err = ini.ReadConfigFile(c.file); err != nil {
		return
	}
	if c.dsn, err = c.cfg.GetString("mysql", "dsn"); err != nil {
		return
	}
	if c.extendedInsRows, err = c.cfg.GetInt("mysql", "extended_insert_rows"); err != nil {
		c.extendedInsRows = 100
	}
	if c.useTableLock, err = c.cfg.GetBool("mysql", "use_table_lock"); err != nil {
		c.useTableLock = true
	}
	if c.maxOpenConns, err = c.cfg.GetInt("mysql", "max_open_conns"); err != nil {
		c.maxOpenConns = 50
	}
	var selects []string
	if selects, err = c.cfg.GetOptions("select"); err != nil {
		return
	}
	for _, tableCol := range selects {
		var table, column string
		if table, column, err = c.splitTableColumn(tableCol); err != nil {
			return
		}
		if c.selectMap[table] == nil {
			c.selectMap[table] = make(map[string]string, 0)
		}
		if c.selectMap[table][column], err = c.cfg.GetString("select", tableCol); err != nil {
			return
		}
	}
	if c.loadOptions("where", c.whereMap); err != nil {
		return
	}
	if c.loadOptions("filter", c.filterMap); err != nil {
		return
	}
	return
}

func (c *config) loadOptions(section string, optMap map[string]string) error {
	var opts []string
	var err error
	if opts, err = c.cfg.GetOptions(section); err != nil {
		return err
	}
	for _, key := range opts {
		if optMap[key], err = c.cfg.GetString(section, key); err != nil {
			return err
		}
	}
	return nil
}

func (c *config) splitTableColumn(tableCol string) (table, column string, err error) {
	split := strings.Split(tableCol, ".")
	if len(split) != 2 {
		err = errors.New("Expected 'table.column' format. Got wrong one:" + tableCol)
		return
	}
	table = split[0]
	column = split[1]
	return
}

func (c *config) initOutput() (*os.File, error) {
	if c.output == UseStdout {
		return os.Stdout, nil
	}
	return os.Create(c.output)
}
