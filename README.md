# MySQL Super Dump

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

This is a fictionary tale based on a true story, but it can happen with lots of 
development teams over the world.

Based on this I've decided to make a program to solve this problem. The first version 
was made in Python 2.7 in a few hours, but I got trapped in text encoding problems of 
Python and MySQLdb library. So I've decided to rewrite it using Go (golang). 

## Usage

* Clone the repository
* Download Go 1.0.3 or newer from http://golang.org, and install it
* Build mysqlduperdump running "go build" inside the project directory
* If you set your Go environment vars correctly, you can even use "go install" instead 
  of "go build", so the binary will be installed in the appropriate bin directory also 
  visible by your $PATH
* Create a mysqlsuperdump.cfg config file and place where you like it. Use the included
  mysqlsuperdump.cfg as an example of setup
* Run mysqlsuperdump -h to see command line options and _voilá_
