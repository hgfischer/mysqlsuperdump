package dumper

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

var logger = log.New(ioutil.Discard, "", 0)

func TestMySQLLockTableRead(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectExec("LOCK TABLES `table` READ").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err = dumper.LockTableReading("table")
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLFlushTable(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectExec("FLUSH TABLES `table`").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err = dumper.FlushTable("table")
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLUnlockTables(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err = dumper.UnlockTables()
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLGetTables(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).
			AddRow("table1", "BASE TABLE").
			AddRow("table2", "BASE TABLE"),
	)
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{"table1", "table2"}, tables)
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLGetTablesHandlingErrorWhenListingTables(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	expectedErr := errors.New("broken")
	sqlmock.ExpectQuery("SHOW FULL TABLES").WillReturnError(expectedErr)
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{}, tables)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, db.Close())
}

func TestMySQLGetTablesHandlingErrorWhenScanningRow(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).AddRow(1, nil))
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{}, tables)
	assert.NotNil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLDumpCreateTable(t *testing.T) {
	var ddl = "CREATE TABLE `table` (" +
		"`id` bigint(20) NOT NULL AUTO_INCREMENT, " +
		"`name` varchar(255) NOT NULL, " +
		"PRIMARY KEY (`id`), KEY `idx_name` (`name`) " +
		") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("table", ddl),
	)
	buffer := bytes.NewBuffer(make([]byte, 0))
	err = dumper.DumpCreateTable(buffer, "table")
	assert.Nil(t, err)
	assert.Contains(t, buffer.String(), "DROP TABLE IF EXISTS `table`")
	assert.Contains(t, buffer.String(), ddl)
	assert.Nil(t, db.Close())
}

func TestMySQLDumpCreateTableHandlingErrorWhenScanningRows(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("table", nil))
	buffer := bytes.NewBuffer(make([]byte, 0))
	err = dumper.DumpCreateTable(buffer, "table")
	assert.NotNil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLGetColumnsForSelect(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	selectMap := map[string]map[string]string{"table": {"col2": "NOW()"}}
	dumper := &MySQL{DB: db, SelectMap: selectMap, Log: logger}
	sqlmock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("a", "b", "c"))
	columns, err := dumper.GetColumnsForSelect("table")
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	assert.Equal(t, []string{"`col1`", "NOW() AS `col2`", "`col3`"}, columns)
}

func TestMySQLGetColumnsForSelectHandlingErrorWhenQuerying(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	selectMap := map[string]map[string]string{"table": {"col2": "NOW()"}}
	dumper := &MySQL{DB: db, SelectMap: selectMap, Log: logger}
	error := errors.New("broken")
	sqlmock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(error)
	columns, err := dumper.GetColumnsForSelect("table")
	assert.Equal(t, err, error)
	assert.Nil(t, db.Close())
	assert.Empty(t, columns)
}

func TestMySQLGetSelectQueryFor(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	selectMap := map[string]map[string]string{"table": {"c2": "NOW()"}}
	whereMap := map[string]string{"table": "c1 > 0"}
	dumper := &MySQL{DB: db, SelectMap: selectMap, WhereMap: whereMap, Log: logger}
	sqlmock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"c1", "c2"}).AddRow("a", "b"))
	query, err := dumper.GetSelectQueryFor("table")
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	assert.Equal(t, "SELECT `c1`, NOW() AS `c2` FROM `table` WHERE c1 > 0", query)
}

func TestMySQLGetSelectQueryForHandlingError(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	selectMap := map[string]map[string]string{"table": {"c2": "NOW()"}}
	whereMap := map[string]string{"table": "c1 > 0"}
	dumper := &MySQL{DB: db, SelectMap: selectMap, WhereMap: whereMap, Log: logger}
	error := errors.New("broken")
	sqlmock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(error)
	query, err := dumper.GetSelectQueryFor("table")
	assert.Equal(t, error, err)
	assert.Nil(t, db.Close())
	assert.Equal(t, "", query)
}

func TestMySQLGetRowCount(t *testing.T) {
	whereMap := map[string]string{"table": "c1 > 0"}
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, WhereMap: whereMap, Log: logger}
	sqlmock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234))
	count, err := dumper.GetRowCount("table")
	assert.Nil(t, err)
	assert.Nil(t, db.Close())
	assert.Equal(t, uint64(1234), count)
}

func TestMySQLGetRowCountHandlingError(t *testing.T) {
	whereMap := map[string]string{"table": "c1 > 0"}
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, WhereMap: whereMap, Log: logger}
	sqlmock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil))
	count, err := dumper.GetRowCount("table")
	assert.NotNil(t, err)
	assert.Nil(t, db.Close())
	assert.Equal(t, uint64(0), count)
}

func TestMySQLDumpTableHeader(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234))
	buffer := bytes.NewBuffer(make([]byte, 0))
	count, err := dumper.DumpTableHeader(buffer, "table")
	assert.Equal(t, uint64(1234), count)
	assert.Nil(t, err)
	assert.Contains(t, buffer.String(), "Data for table `table`")
	assert.Contains(t, buffer.String(), "1234 rows")
	assert.Nil(t, db.Close())
}

func TestMySQLDumpTableHeaderHandlingError(t *testing.T) {
	db, err := sqlmock.New()
	assert.Nil(t, err)
	dumper := &MySQL{DB: db, Log: logger}
	sqlmock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil))
	buffer := bytes.NewBuffer(make([]byte, 0))
	count, err := dumper.DumpTableHeader(buffer, "table")
	assert.Equal(t, uint64(0), count)
	assert.NotNil(t, err)
	assert.Nil(t, db.Close())
}

func TestMySQLDumpTableLockWrite(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := &MySQL{Log: logger}
	dumper.DumpTableLockWrite(buffer, "table")
	assert.Contains(t, buffer.String(), "LOCK TABLES `table` WRITE;")
}

func TestMySQLDumpUnlockTables(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := &MySQL{Log: logger}
	dumper.DumpUnlockTables(buffer)
	assert.Contains(t, buffer.String(), "UNLOCK TABLES;")
}
