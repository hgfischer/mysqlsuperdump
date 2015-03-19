# MySQL Super Dump

*MySQL Super Dump* is a tool to efficiently create filtered and manipulated database dumps. It relies in the power
of the SQL native language to do this, using WHERE clauses and complete SELECT statements with aliases to do this.

Currently it does not support every kind of MySQL structure (views, triggers, etc), but it supports the most basic 
stuff: schemas, tables and rows.

## History

> Once uppon a time there was a development team that liked to use dumps from the
production database in their development environments to have the same content
and behavior of the production system in their machines.

> To avoid security problems, the system administrator created a script to dump
the production database, import in a temporary database, then replace all 
sensitive data, like salts, passwords, customer names, emails, etc, for fake
data, then export a dump of this temporary database to a file that is the dump
developers would use.

> However this script was taking more time to run, day by day, and each day it was
using more resources from the server to run, until it exploded.


## Features

* Filter dumped rows by a native WHERE clause (`[where]` config's section)
* Replace dumped data with native SELECT functions (`[select]` config's section)
* Disable data output of specific tables (`[filter]` config's section: `nodata`)
* Ignore entire tables (`[filter]` config's section: `ignore`)


## Usage

* Install the latest Go compiler installed (check instructions at: http://golang.org)
* Check you environment with `go env`:
 * The repository will be clones at `$GOPATH/src/github.com/hgfischer/mysqlsuperdump`
 * The binary will be installed in `$GOBIN`
* Then run `go get` to download, build and install `mysqlsuperdump`: `go get github.com/hgfischer/mysqlsuperdump`
* Create a config file based on `example.cfg` and place where you like it.
* Run mysqlsuperdump -h to see command line options and _voilá_.


## TO DO

* Extend MySQL support, with other objects like views, triggers, etc
* Refactor dumper interface to support another SQL databases
* Add support for PostgreSQL


## License

Please, check the LICENSE file.
