package prequel

import (
	"context"
	"database/sql"

	"github.com/syreclabs/sqlx"
	"syreclabs.com/go/loggie"
	"syreclabs.com/go/prequel/builder"
)

type Queryer interface {
	sqlx.QueryerContext
}

type Execer interface {
	sqlx.ExecerContext
}

var log = loggie.New("sql")

func SetLogger(logger loggie.Logger) {
	log = logger
}

func Select(ctx context.Context, q Queryer, b builder.Builder, dest interface{}) error {
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	log.Infof("%q %v", sql, params)
	return sqlx.SelectContext(ctx, q, dest, sql, params...)
}

func Get(ctx context.Context, q Queryer, b builder.Builder, dest interface{}) error {
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	log.Infof("%q %v", sql, params)
	return sqlx.GetContext(ctx, q, dest, sql, params...)
}

func Exec(ctx context.Context, e Execer, b builder.Builder) (sql.Result, error) {
	sql, params, err := b.Build()
	if err != nil {
		return nil, err
	}
	log.Infof("%q %v", sql, params)
	return e.ExecContext(ctx, sql, params...)
}

func MustExec(ctx context.Context, e Execer, b builder.Builder) sql.Result {
	res, err := Exec(ctx, e, b)
	if err != nil {
		panic(err)
	}
	return res
}

// DB is a wrapper around sqlx.DB which supports builder.Builder.
type DB struct {
	DB *sqlx.DB
}

func NewDB(db *sql.DB, driverName string) *DB {
	return &DB{sqlx.NewDb(db, driverName)}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	sqlxdb, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{sqlxdb}, nil
}

func MustOpen(driverName, dataSourceName string) *DB {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

func Connect(ctx context.Context, driverName, dataSourceName string) (*DB, error) {
	sqlxdb, err := sqlx.ConnectContext(ctx, driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{sqlxdb}, nil
}

func MustConnect(ctx context.Context, driverName, dataSourceName string) *DB {
	db, err := Connect(ctx, driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

func (db *DB) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Select(ctx, db.DB, b, dest)
}

func (db *DB) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Get(ctx, db.DB, b, dest)
}

func (db *DB) Exec(ctx context.Context, b builder.Builder) (sql.Result, error) {
	return Exec(ctx, db.DB, b)
}

func (db *DB) MustExec(ctx context.Context, b builder.Builder) sql.Result {
	return MustExec(ctx, db.DB, b)
}

func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	sqlxtx, err := db.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	sqlxtx, err := db.DB.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

func (db *DB) MustBegin(ctx context.Context) *Tx {
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	return tx
}

func (db *DB) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		panic(err)
	}
	return tx
}

// Tx is a wrapper around sqlx.Tx which supports builder.Builder.
type Tx struct {
	Tx *sqlx.Tx
}

func (tx *Tx) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Select(ctx, tx.Tx, b, dest)
}

func (tx *Tx) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Get(ctx, tx.Tx, b, dest)
}

func (tx *Tx) Exec(ctx context.Context, b builder.Selecter) (sql.Result, error) {
	return Exec(ctx, tx.Tx, b)
}

func (tx *Tx) MustExec(ctx context.Context, b builder.Selecter) sql.Result {
	return MustExec(ctx, tx.Tx, b)
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}

// Conn is a wrapper around sqlx.Conn which supports builder.Builder.
type Conn struct {
	Conn *sqlx.Conn
}

func (conn *Conn) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Select(ctx, conn.Conn, b, dest)
}

// Stmt is a wrapper around sqlx.Stmt which supports builder.Builder.
type Stmt struct {
	Stmt *sqlx.Stmt
}

func (stmt *Stmt) Select(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Select(ctx, stmtWrapper{stmt.Stmt}, b, dest)
}

func (stmt *Stmt) Get(ctx context.Context, b builder.Builder, dest interface{}) error {
	return Get(ctx, stmtWrapper{stmt.Stmt}, b, dest)
}

func (stmt *Stmt) Exec(ctx context.Context, b builder.Selecter) (sql.Result, error) {
	return Exec(ctx, stmtWrapper{stmt.Stmt}, b)
}

func (stmt *Stmt) MustExec(ctx context.Context, b builder.Selecter) sql.Result {
	return MustExec(ctx, stmtWrapper{stmt.Stmt}, b)
}

// stmtWrapper is an unexported wrapper which implements Queryer and Execer by
// delegating to the underlying sqlx.Stmt.
type stmtWrapper struct{ *sqlx.Stmt }

func (w stmtWrapper) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return w.Stmt.QueryContext(ctx, args...)
}

func (w stmtWrapper) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return w.Stmt.QueryxContext(ctx, args...)
}

func (w stmtWrapper) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	return w.Stmt.QueryRowxContext(ctx, args...)
}

func (w stmtWrapper) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return w.Stmt.ExecContext(ctx, args...)
}
