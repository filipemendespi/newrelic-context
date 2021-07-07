package nrgorm

import (
	"fmt"
	"github.com/newrelic/go-agent/v3/newrelic"
	"gorm.io/gorm"
	"strings"
)

const (
	txnGormKey   = "newrelicTransaction"
	startTimeKey = "newrelicStartTime"
)

func SetTxnToGorm(txn *newrelic.Transaction, db *gorm.DB) *gorm.DB {
	if txn == nil {
		return db
	}
	return db.Set(txnGormKey, txn)
}

// AddGormCallbacks adds callbacks to NewRelic, you should call SetTxnToGorm to make them work
func AddGormCallbacks(db *gorm.DB) {
	dialect := db.Config.Dialector.Name()

	var product newrelic.DatastoreProduct
	switch dialect {
	case "postgres":
		product = newrelic.DatastorePostgres
	case "mysql":
		product = newrelic.DatastoreMySQL
	case "sqlite3":
		product = newrelic.DatastoreSQLite
	case "mssql":
		product = newrelic.DatastoreMSSQL
	default:
		return
	}
	callbacks := newCallbacks(product)
	registerCallbacks(db, "transaction", callbacks)
	registerCallbacks(db, "create", callbacks)
	registerCallbacks(db, "query", callbacks)
	registerCallbacks(db, "update", callbacks)
	registerCallbacks(db, "delete", callbacks)
	registerCallbacks(db, "row", callbacks)
}

type callbacks struct {
	product newrelic.DatastoreProduct
}

func newCallbacks(product newrelic.DatastoreProduct) *callbacks {
	return &callbacks{product}
}

func (c *callbacks) beforeCreate(DB *gorm.DB) { c.before(DB) }
func (c *callbacks) afterCreate(DB *gorm.DB)  { c.after(DB, "INSERT") }
func (c *callbacks) beforeQuery(DB *gorm.DB)  { c.before(DB) }
func (c *callbacks) afterQuery(DB *gorm.DB)   { c.after(DB, "SELECT") }
func (c *callbacks) beforeUpdate(DB *gorm.DB) { c.before(DB) }
func (c *callbacks) afterUpdate(DB *gorm.DB)  { c.after(DB, "UPDATE") }
func (c *callbacks) beforeDelete(DB *gorm.DB) { c.before(DB) }
func (c *callbacks) afterDelete(DB *gorm.DB)  { c.after(DB, "DELETE") }
func (c *callbacks) beforeRow(DB *gorm.DB)    { c.before(DB) }
func (c *callbacks) afterRow(DB *gorm.DB)     { c.after(DB, "") }

func (c *callbacks) before(DB *gorm.DB) {
	txn, ok := DB.Get(txnGormKey)
	if !ok {
		return
	}
	DB.Set(startTimeKey, newrelic.StartSegmentNow(txn.(*newrelic.Transaction)))
}

func (c *callbacks) after(DB *gorm.DB, operation string) {
	startTime, ok := DB.Get(startTimeKey)

	if !ok {
		return
	}
	if operation == "" {
		operation = strings.ToUpper(strings.Split(DB.Statement.SQL.String(), " ")[0])
	}
	segmentBuilder(
		startTime.(newrelic.SegmentStartTime),
		c.product,
		DB.Statement.SQL.String(),
		operation,
		DB.Statement.Table,
	).End()

	// gorm wraps insert&update into transaction automatically
	// add another segment for commit/rollback in such case
	if _, ok := DB.InstanceGet("gorm:started_transaction"); !ok {
		DB.Set(startTimeKey, nil)
		return
	}

	txn, _ := DB.Get(txnGormKey)
	DB.Set(startTimeKey, newrelic.StartSegmentNow(txn.(*newrelic.Transaction)))
}

func (c *callbacks) commitOrRollback(DB *gorm.DB) {
	startTime, ok := DB.Get(startTimeKey)
	if !ok || startTime == nil {
		return
	}

	segmentBuilder(
		startTime.(newrelic.SegmentStartTime),
		c.product,
		"",
		"COMMIT/ROLLBACK",
		DB.Statement.Table,
	).End()
}

func registerCallbacks(db *gorm.DB, name string, c *callbacks) {
	beforeName := fmt.Sprintf("newrelic:%v_before", name)
	afterName := fmt.Sprintf("newrelic:%v_after", name)
	gormCallbackName := fmt.Sprintf("gorm:%v", name)

	// gorm does some magic, if you pass CallbackProcessor here - nothing works
	switch name {
	case "create":
		db.Callback().Create().Before(gormCallbackName).Register(beforeName, c.beforeCreate)
		db.Callback().Create().After(gormCallbackName).Register(afterName, c.afterCreate)
		db.Callback().Create().
			After("gorm:commit_or_rollback_transaction").
			Register(fmt.Sprintf("newrelic:commit_or_rollback_transaction_%v", name), c.commitOrRollback)
	case "query":
		db.Callback().Query().Before(gormCallbackName).Register(beforeName, c.beforeQuery)
		db.Callback().Query().After(gormCallbackName).Register(afterName, c.afterQuery)
	case "update":
		db.Callback().Update().Before(gormCallbackName).Register(beforeName, c.beforeUpdate)
		db.Callback().Update().After(gormCallbackName).Register(afterName, c.afterUpdate)
		db.Callback().Update().
			After("gorm:commit_or_rollback_transaction").
			Register(fmt.Sprintf("newrelic:commit_or_rollback_transaction_%v", name), c.commitOrRollback)
	case "delete":
		db.Callback().Delete().Before(gormCallbackName).Register(beforeName, c.beforeDelete)
		db.Callback().Delete().After(gormCallbackName).Register(afterName, c.afterDelete)
		db.Callback().Delete().
			After("gorm:commit_or_rollback_transaction").
			Register(fmt.Sprintf("newrelic:commit_or_rollback_transaction_%v", name), c.commitOrRollback)
	case "row":
		db.Callback().Row().Before(gormCallbackName).Register(beforeName, c.beforeRow)
		db.Callback().Row().After(gormCallbackName).Register(afterName, c.afterRow)
	}
}

type segment interface {
	End()
}

var segmentBuilder = func(
	startTime newrelic.SegmentStartTime,
	product newrelic.DatastoreProduct,
	query string,
	operation string,
	collection string,
) segment {
	return &newrelic.DatastoreSegment{
		StartTime:          startTime,
		Product:            product,
		ParameterizedQuery: query,
		Operation:          operation,
		Collection:         collection,
	}
}
