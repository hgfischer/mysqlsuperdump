package dumper

import (
	"bytes"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func getDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)
	return db, mock
}

func TestMySQLLockTableRead(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectExec("LOCK TABLES `table` READ").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.LockTableReading("table")
	assert.Nil(t, err)
}

func TestMySQLFlushTable(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectExec("FLUSH TABLES `table`").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.FlushTable("table")
	assert.Nil(t, err)
}

func TestMySQLUnlockTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectExec("UNLOCK TABLES").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := dumper.UnlockTables()
	assert.Nil(t, err)
}

func TestMySQLGetTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).
			AddRow("table1", "BASE TABLE").
			AddRow("table2", "BASE TABLE"),
	)
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{"table1", "table2"}, tables)
	assert.Nil(t, err)
}

func TestMySQLGetTablesHandlingErrorWhenListingTables(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	expectedErr := errors.New("broken")
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnError(expectedErr)
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{}, tables)
	assert.Equal(t, expectedErr, err)
}

func TestMySQLGetTablesHandlingErrorWhenScanningRow(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).AddRow(1, nil))
	tables, err := dumper.GetTables()
	assert.Equal(t, []string{}, tables)
	assert.NotNil(t, err)
}

func TestMySQLDumpCreateTable(t *testing.T) {
	var ddl = "CREATE TABLE `table` (" +
		"`id` bigint(20) NOT NULL AUTO_INCREMENT, " +
		"`name` varchar(255) NOT NULL, " +
		"PRIMARY KEY (`id`), KEY `idx_name` (`name`) " +
		") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("table", ddl),
	)
	buffer := bytes.NewBuffer(make([]byte, 0))
	assert.Nil(t, dumper.DumpCreateTable(buffer, "table"))
	assert.Contains(t, buffer.String(), "DROP TABLE IF EXISTS `table`")
	assert.Contains(t, buffer.String(), ddl)
}

func TestMySQLDumpCreateTableHandlingErrorWhenScanningRows(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SHOW CREATE TABLE `table`").WillReturnRows(
		sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("table", nil))
	buffer := bytes.NewBuffer(make([]byte, 0))
	assert.NotNil(t, dumper.DumpCreateTable(buffer, "table"))
}

func TestMySQLGetColumnsForSelect(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.SelectMap = map[string]map[string]string{"table": {"col2": "NOW()"}}
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"col1", "col2", "col3"}).AddRow("a", "b", "c"))
	columns, err := dumper.GetColumnsForSelect("table")
	assert.Nil(t, err)
	assert.Equal(t, []string{"`col1`", "NOW() AS `col2`", "`col3`"}, columns)
}

func TestMySQLGetColumnsForSelectHandlingErrorWhenQuerying(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.SelectMap = map[string]map[string]string{"table": {"col2": "NOW()"}}
	error := errors.New("broken")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(error)
	columns, err := dumper.GetColumnsForSelect("table")
	assert.Equal(t, err, error)
	assert.Empty(t, columns)
}

func TestMySQLGetSelectQueryFor(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.SelectMap = map[string]map[string]string{"table": {"c2": "NOW()"}}
	dumper.WhereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"c1", "c2"}).AddRow("a", "b"))
	query, err := dumper.GetSelectQueryFor("table")
	assert.Nil(t, err)
	assert.Equal(t, "SELECT `c1`, NOW() AS `c2` FROM `table` WHERE c1 > 0", query)
}

func TestMySQLGetSelectQueryForHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.SelectMap = map[string]map[string]string{"table": {"c2": "NOW()"}}
	dumper.WhereMap = map[string]string{"table": "c1 > 0"}
	error := errors.New("broken")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(error)
	query, err := dumper.GetSelectQueryFor("table")
	assert.Equal(t, error, err)
	assert.Equal(t, "", query)
}

func TestMySQLGetRowCount(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.WhereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234))
	count, err := dumper.GetRowCount("table")
	assert.Nil(t, err)
	assert.Equal(t, uint64(1234), count)
}

func TestMySQLGetRowCountHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	dumper.WhereMap = map[string]string{"table": "c1 > 0"}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table` WHERE c1 > 0").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil))
	count, err := dumper.GetRowCount("table")
	assert.NotNil(t, err)
	assert.Equal(t, uint64(0), count)
}

func TestMySQLDumpTableHeader(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1234))
	buffer := bytes.NewBuffer(make([]byte, 0))
	count, err := dumper.DumpTableHeader(buffer, "table")
	assert.Equal(t, uint64(1234), count)
	assert.Nil(t, err)
	assert.Contains(t, buffer.String(), "Data for table `table`")
	assert.Contains(t, buffer.String(), "1234 rows")
}

func TestMySQLDumpTableHeaderHandlingError(t *testing.T) {
	db, mock := getDB(t)
	dumper := NewMySQLDumper(db, nil)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(nil))
	buffer := bytes.NewBuffer(make([]byte, 0))
	count, err := dumper.DumpTableHeader(buffer, "table")
	assert.Equal(t, uint64(0), count)
	assert.NotNil(t, err)
}

func TestMySQLDumpTableLockWrite(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := NewMySQLDumper(nil, nil)
	dumper.DumpTableLockWrite(buffer, "table")
	assert.Contains(t, buffer.String(), "LOCK TABLES `table` WRITE;")
}

func TestMySQLDumpUnlockTables(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := NewMySQLDumper(nil, nil)
	dumper.DumpUnlockTables(buffer)
	assert.Contains(t, buffer.String(), "UNLOCK TABLES;")
}

func TestMySQLDumpTableData(t *testing.T) {
	db, mock := getDB(t)
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := NewMySQLDumper(db, nil)
	dumper.ExtendedInsertRows = 2

	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"id", "language"}).
			AddRow(1, "Go"))

	mock.ExpectQuery("SELECT `id`, `language` FROM `table`").WillReturnRows(
		sqlmock.NewRows([]string{"id", "language"}).
			AddRow(1, "Go").
			AddRow(2, "Java").
			AddRow(3, "C").
			AddRow(4, "C++").
			AddRow(5, "Rust").
			AddRow(6, "Closure"))

	assert.Nil(t, dumper.DumpTableData(buffer, "table"))

	assert.Equal(t, strings.Count(buffer.String(), "INSERT INTO `table` VALUES"), 3)
	assert.Contains(t, buffer.String(), `'Go'`)
	assert.Contains(t, buffer.String(), `'Java'`)
	assert.Contains(t, buffer.String(), `'C'`)
	assert.Contains(t, buffer.String(), `'C++'`)
	assert.Contains(t, buffer.String(), `'Rust'`)
	assert.Contains(t, buffer.String(), `'Closure'`)
}

func TestMySQLDumpTableDataHandlingErrorFromSelectAllDataFor(t *testing.T) {
	db, mock := getDB(t)
	buffer := bytes.NewBuffer(make([]byte, 0))
	dumper := NewMySQLDumper(db, nil)
	error := errors.New("fail")
	mock.ExpectQuery("SELECT \\* FROM `table` LIMIT 1").WillReturnError(error)
	assert.Equal(t, error, dumper.DumpTableData(buffer, "table"))
}

// WIP
// TODO Replace all tests by an integration test or create a better database/sql mock
//func TestMySQLDump(t *testing.T) {
//    db, mock := getDB(t)
//    buffer := bytes.NewBuffer(make([]byte, 0))
//    dumper := NewMySQLDumper(db, nil)

//    mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(
//        sqlmock.NewRows([]string{"Tables_in_database", "Table_type"}).
//            AddRow("table1", "BASE TABLE"))

//    mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `table1`").WillReturnRows(
//        sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2))

//    assert.Nil(t, dumper.Dump(buffer))
//}
