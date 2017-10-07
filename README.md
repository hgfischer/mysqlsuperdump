# MySQL Super Dump

[![Build Status](https://travis-ci.org/hgfischer/mysqlsuperdump.svg?branch=master)](https://travis-ci.org/hgfischer/mysqlsuperdump)
[![Go Report Card](https://goreportcard.com/badge/hgfischer/mysqlsuperdump)](https://goreportcard.com/report/hgfischer/mysqlsuperdump)

*MySQL Super Dump* is a tool to efficiently create *filtered* and *manipulated* database dumps. It relies in the power
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


## Configuration Example

```

[mysql]
# See https://github.com/Go-SQL-Driver/MySQL for details on this
dsn = username:password@protocol(address)/dbname?charset=utf8
extended_insert_rows = 1000
#use_table_lock = true
max_open_conns = 50

# Use this to restrict exported data. These are optional
[where]
sales_order           = created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
customer_upload       = created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
newsletter_subscriber = created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)

# Use this to override value returned from tables. These are optional
[select]
system_user.salt = 'reset salt of all system users'
system_user.password = 'reset password of all system users'

customer.first_name = CONCAT('Charlie ', id)
customer.last_name = 'Last'
customer.salt = 'reset salt of all customers'
customer.password = 'reset password of all customers'
customer.username = CONCAT(id, '@fiction.tld')
customer.username_canonical = CONCAT(id, '@fiction.tld')
customer.email = CONCAT(id, '@fiction.tld')
customer.email_canonical = CONCAT(id, '@fiction.tld')

newsletter_subscriber.email = CONCAT(id, '@fiction.tld')

customer_address.recipient_name = CONCAT('Recipient Name ', id)
customer_address.company = CONCAT('Company Name ', id)
customer_address.phone = CONCAT('(', id, ') 1234-1234')

sales_order_address.recipient_name = CONCAT('Recipient Name ', id)
sales_order_address.company = CONCAT('Company Name ', id)
sales_order_address.phone = CONCAT('(', id, ') 1234-1234')

system_dump_version.created_at = NOW()

# Use this to filter entire table (ignore) or data only (nodata)
[filter]
customer_stats = nodata
customer_private = ignore
```

## TO DO

* Extend MySQL support, with other objects like views, triggers, etc
* Refactor dumper interface to support another SQL databases
* Add support for PostgreSQL


## License

Please, check the LICENSE file.
