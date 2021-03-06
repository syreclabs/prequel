// Package prequel provides PostgreSQL query bulder and executor.
package prequel // import "syreclabs.com/go/prequel"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"syreclabs.com/go/prequel/builder"
)

// Queryer is an interface used by Select and Get
type Queryer interface {
	Select(ctx context.Context, b builder.Builder, dest interface{}) error
	SelectRaw(ctx context.Context, dest interface{}, q string, params ...interface{}) error
	Get(ctx context.Context, b builder.Builder, dest interface{}) error
	GetRaw(ctx context.Context, dest interface{}, q string, params ...interface{}) error
}

// Execer is an interface used by Exec and MustExec.
type Execer interface {
	Exec(ctx context.Context, b builder.Builder) (sql.Result, error)
	ExecRaw(ctx context.Context, query string, params ...interface{}) (sql.Result, error)
	MustExec(ctx context.Context, b builder.Builder) sql.Result
	MustExecRaw(ctx context.Context, query string, params ...interface{}) sql.Result
}

// Runner is an interface used by both Queryer and Execer.
type Runner interface {
	Queryer
	Execer
}

// Beginner is an interface used by Begin and BeginTx (and their Must* variants).
type Beginner interface {
	Begin(ctx context.Context) (*Tx, error)
	MustBegin(ctx context.Context) *Tx
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
	MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx
}

var log = newDefaultLogger()

// SetLogger allows changing logging adapter used by prequel.
func SetLogger(logger Logger) {
	log = logger
}

func SetLogLevel(lvl int) {
	log.SetLevel(lvl)
}

// DB is a wrapper around sqlx.DB which supports builder.Builder.
type DB struct {
	DB *sqlx.DB
}

// NewDB is a wrapper for sqlx.NewDb that returns *prequel.DB.
func NewDB(db *sql.DB, driverName string) *DB {
	return &DB{sqlx.NewDb(db, driverName)}
}

// Open is a wrapper for sqlx.Open that returns *prequel.DB.
func Open(driverName, dataSourceName string) (*DB, error) {
	sqlxdb, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{sqlxdb}, nil
}

// MustOpen is a wrapper for sqlx.MustOpen that returns *prequel.DB.
// This method will panic on error.
func MustOpen(driverName, dataSourceName string) *DB {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// Connect is a wrapper for sqlx.Connect that returns *prequel.DB.
func Connect(ctx context.Context, driverName, dataSourceName string) (*DB, error) {
	sqlxdb, err := sqlx.ConnectContext(ctx, driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{sqlxdb}, nil
}

// MustConnect is a wrapper for sqlx.MustConnect that returns *prequel.DB.
// This method will panic on error.
func MustConnect(ctx context.Context, driverName, dataSourceName string) *DB {
	db, err := Connect(ctx, driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// Select using this DB.
func (db *DB) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doSelect(ctx, db.DB, b, dest)
}

func (db *DB) SelectRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doSelectRaw(ctx, db.DB, dest, sql, params...)
}

// Get using this DB.
func (db *DB) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doGet(ctx, db.DB, b, dest)
}

func (db *DB) GetRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doGetRaw(ctx, db.DB, dest, sql, params...)
}

// Exec using this DB.
func (db *DB) Exec(ctx context.Context, b builder.Builder) (sql.Result, error) {
	return doExec(ctx, db.DB, b)
}

func (db *DB) ExecRaw(ctx context.Context, sql string, params ...interface{}) (sql.Result, error) {
	return doExecRaw(ctx, db.DB, sql, params...)
}

// MustExec using this DB. This method will panic on error.
func (db *DB) MustExec(ctx context.Context, b builder.Builder) sql.Result {
	return doMustExec(ctx, db.DB, b)
}

func (db *DB) MustExecRaw(ctx context.Context, sql string, params ...interface{}) sql.Result {
	return doMustExecRaw(ctx, db.DB, sql, params...)
}

// Begin starts a new transaction using this DB.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	defer logf(time.Now(), "BEGIN")
	sqlxtx, err := db.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

// BeginTx starts a new transaction using this DB.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if opts != nil {
		defer logf(time.Now(), "BEGIN [opts: %#v]", opts)
	} else {
		defer logf(time.Now(), "BEGIN")
	}
	sqlxtx, err := db.DB.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

// MustBegin starts a new transaction using this DB. This method will panic on error.
func (db *DB) MustBegin(ctx context.Context) *Tx {
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginTx starts a new transaction using this DB. This method will panic on error.
func (db *DB) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// Conn returns a single connection using this DB.
// Conn will block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
// Every Conn must be returned to the database pool after use by calling Conn.Close.
func (db *DB) Conn(ctx context.Context) (*Conn, error) {
	sqlxconn, err := db.DB.Connx(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{sqlxconn}, nil
}

// Conn returns a single connection using this DB and panic on error.
// Conn will block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
// Every Conn must be returned to the database pool after use by calling Conn.Close.
func (db *DB) MustConn(ctx context.Context) *Conn {
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	return conn
}

// Tx is a wrapper around sqlx.Tx which supports builder.Builder.
type Tx struct {
	Tx *sqlx.Tx
}

// Select using this transaction.
func (tx *Tx) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doSelect(ctx, tx.Tx, b, dest)
}

func (tx *Tx) SelectRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doSelectRaw(ctx, tx.Tx, dest, sql, params...)
}

// Get using this transaction.
func (tx *Tx) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doGet(ctx, tx.Tx, b, dest)
}

func (tx *Tx) GetRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doGetRaw(ctx, tx.Tx, dest, sql, params...)
}

// Exec using this transaction.
func (tx *Tx) Exec(ctx context.Context, b builder.Builder) (sql.Result, error) {
	return doExec(ctx, tx.Tx, b)
}

func (tx *Tx) ExecRaw(ctx context.Context, sql string, params ...interface{}) (sql.Result, error) {
	return doExecRaw(ctx, tx.Tx, sql, params...)
}

// Must Exec using this transaction and panic on error.
func (tx *Tx) MustExec(ctx context.Context, b builder.Builder) sql.Result {
	return doMustExec(ctx, tx.Tx, b)
}

func (tx *Tx) MustExecRaw(ctx context.Context, sql string, params ...interface{}) sql.Result {
	return doMustExecRaw(ctx, tx.Tx, sql, params...)
}

// Commit this transaction.
func (tx *Tx) Commit() error {
	defer logf(time.Now(), "COMMIT")
	return tx.Tx.Commit()
}

// Rollback this transaction.
func (tx *Tx) Rollback() error {
	defer logf(time.Now(), "ROLLBACK")
	return tx.Tx.Rollback()
}

// Conn is a wrapper around sqlx.Conn which supports builder.Builder.
type Conn struct {
	Conn *sqlx.Conn
}

// Close returns this connection to the connection pool.
func (conn *Conn) Close() error {
	return conn.Conn.Close()
}

// Select using this connection.
func (conn *Conn) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doSelect(ctx, conn.Conn, b, dest)
}

func (conn *Conn) SelectRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doSelectRaw(ctx, conn.Conn, dest, sql, params...)
}

// Get using this connection.
func (conn *Conn) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return doGet(ctx, conn.Conn, b, dest)
}

func (conn *Conn) GetRaw(ctx context.Context, dest interface{}, sql string, params ...interface{}) error {
	return doGetRaw(ctx, conn.Conn, dest, sql, params...)
}

// Exec using this connection.
func (conn *Conn) Exec(ctx context.Context, b builder.Builder) (sql.Result, error) {
	return doExec(ctx, conn.Conn, b)
}

func (conn *Conn) ExecRaw(ctx context.Context, sql string, params ...interface{}) (sql.Result, error) {
	return doExecRaw(ctx, conn.Conn, sql, params...)
}

// MustExec using this connection. This method will panic on error.
func (conn *Conn) MustExec(ctx context.Context, b builder.Builder) sql.Result {
	return doMustExec(ctx, conn.Conn, b)
}

func (conn *Conn) MustExecRaw(ctx context.Context, sql string, params ...interface{}) sql.Result {
	return doMustExecRaw(ctx, conn.Conn, sql, params...)
}

// Begin starts a new transaction using this connection.
func (conn *Conn) Begin(ctx context.Context) (*Tx, error) {
	defer logf(time.Now(), "BEGIN")
	sqlxtx, err := conn.Conn.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

// BeginTx starts a new transaction using this connection.
func (conn *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if opts != nil {
		defer logf(time.Now(), "BEGIN [Isolation:%v ReadOnly:%v]", opts.Isolation, opts.ReadOnly)
	} else {
		defer logf(time.Now(), "BEGIN [nil opts]")
	}
	sqlxtx, err := conn.Conn.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

// MustBegin starts a new transaction using this DB. This method will panic on error.
func (conn *Conn) MustBegin(ctx context.Context) *Tx {
	tx, err := conn.Begin(ctx)
	if err != nil {
		panic(err)
	}
	return tx
}

// MustBeginTx starts a new transaction using this DB. This method will panic on error.
func (conn *Conn) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// // Stmt is a wrapper around sqlx.Stmt which supports builder.Builder.
// type Stmt struct {
// 	Stmt *sqlx.Stmt
// }

// // Select using this Stmt.
// func (stmt *Stmt) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
// 	return doSelect(ctx, stmtWrapper{stmt.Stmt}, b, dest)
// }

// // Get using this Stmt.
// func (stmt *Stmt) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
// 	return doGet(ctx, stmtWrapper{stmt.Stmt}, b, dest)
// }

// // Exec using this Stmt.
// func (stmt *Stmt) Exec(ctx context.Context, b builder.Builder) (sql.Result, error) {
// 	return doExec(ctx, stmtWrapper{stmt.Stmt}, b)
// }

// // MustExec using this Stmt. This method will panic on error.
// func (stmt *Stmt) MustExec(ctx context.Context, b builder.Builder) sql.Result {
// 	return doMustExec(ctx, stmtWrapper{stmt.Stmt}, b)
// }

// // stmtWrapper is an unexported wrapper which implements Queryer and Execer by
// // delegating to the underlying sqlx.Stmt.
// type stmtWrapper struct{ *sqlx.Stmt }

// func (w stmtWrapper) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
// 	return w.Stmt.QueryContext(ctx, args...)
// }

// func (w stmtWrapper) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
// 	return w.Stmt.QueryxContext(ctx, args...)
// }

// func (w stmtWrapper) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
// 	return w.Stmt.QueryRowxContext(ctx, args...)
// }

// func (w stmtWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
// 	return w.Stmt.ExecContext(ctx, args...)
// }

// doSelect builds the query using the provided builder, executes it with queryer and
// scans each row into dest, which must be a slice. If the slice elements are scannable,
// then the result set must have only one column. Otherwise, sqlx.StructScan is used.
func doSelect(ctx context.Context, q sqlx.QueryerContext, b builder.Builder, dest interface{}) error {
	start := time.Now()
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	defer logSql(start, sql, params)
	return sqlx.SelectContext(ctx, q, dest, sql, params...)
}

func doSelectRaw(ctx context.Context, q sqlx.QueryerContext, dest interface{}, sql string, params ...interface{}) error {
	start := time.Now()
	defer logSql(start, sql, params)
	return sqlx.SelectContext(ctx, q, dest, sql, params...)
}

// doGet builds the query using the provided builder, executes it with queryer and scans the
// resulting row to dest. If dest is scannable, the result must only have one column. Otherwise,
// sqlx.StructScan is used. Get will return sql.ErrNoRows if the result set is empty.
func doGet(ctx context.Context, q sqlx.QueryerContext, b builder.Builder, dest interface{}) error {
	start := time.Now()
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	defer logSql(start, sql, params)
	return sqlx.GetContext(ctx, q, dest, sql, params...)
}

func doGetRaw(ctx context.Context, q sqlx.QueryerContext, dest interface{}, sql string, params ...interface{}) error {
	start := time.Now()
	defer logSql(start, sql, params)
	return sqlx.GetContext(ctx, q, dest, sql, params...)
}

// doExec builds the query using the provided builder and executes it with execer.
func doExec(ctx context.Context, e sqlx.ExecerContext, b builder.Builder) (sql.Result, error) {
	start := time.Now()
	sql, params, err := b.Build()
	if err != nil {
		return nil, err
	}
	defer logSql(start, sql, params)
	return e.ExecContext(ctx, sql, params...)
}

func doExecRaw(ctx context.Context, e sqlx.ExecerContext, sql string, params ...interface{}) (sql.Result, error) {
	start := time.Now()
	defer logSql(start, sql, params)
	return e.ExecContext(ctx, sql, params...)
}

// doMustExec builds the query using the provided builder and executes it with execer.
// It will panic if there was an error.
func doMustExec(ctx context.Context, e sqlx.ExecerContext, b builder.Builder) sql.Result {
	res, err := doExec(ctx, e, b)
	if err != nil {
		panic(err)
	}
	return res
}

func doMustExecRaw(ctx context.Context, e sqlx.ExecerContext, sql string, params ...interface{}) sql.Result {
	res, err := doExecRaw(ctx, e, sql, params...)
	if err != nil {
		panic(err)
	}
	return res
}

func logf(start time.Time, format string, args ...interface{}) {
	elapsed := time.Since(start)
	log.Printf("%s %v", fmt.Sprintf(format, args...), elapsed)
}

func logSql(start time.Time, sql string, params []interface{}) {
	elapsed := time.Since(start)
	log.Printf("%s %v %v", sql, params, elapsed)
}
