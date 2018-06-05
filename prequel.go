package prequel

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"syreclabs.com/dg/loggie"
	"syreclabs.com/go/prequel/builder"
)

var log = loggie.New("sql")

type Queryer interface {
	sqlx.QueryerContext
}

type Execer interface {
	sqlx.ExecerContext
}

func Select(ctx context.Context, q Queryer, b builder.Selecter, dest interface{}) error {
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	log.Printf(loggie.Linfo, "%q %#v", sql, params)
	return sqlx.SelectContext(ctx, q, dest, sql, params...)
}

func Exec(ctx context.Context, e Execer, b builder.Builder) (sql.Result, error) {
	sql, params, err := b.Build()
	if err != nil {
		return nil, err
	}
	log.Printf(loggie.Ldebug, "sql: %q params: %v", sql, params)
	return e.ExecContext(ctx, sql, params...)
}

func MustExec(ctx context.Context, e Execer, b builder.Builder) sql.Result {
	res, err := Exec(ctx, e, b)
	if err != nil {
		panic(err)
	}
	return res
}

type DB struct {
	*sqlx.DB
}

func (db *DB) Select(ctx context.Context, b builder.Selecter, dest interface{}) error {
	return Select(ctx, db, b, dest)
}

func (db *DB) Exec(ctx context.Context, b builder.Selecter) (sql.Result, error) {
	return Exec(ctx, db, b)
}

func (db *DB) MustExec(ctx context.Context, b builder.Selecter) sql.Result {
	return MustExec(ctx, db, b)
}

func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	sqlxtx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	return &Tx{sqlxtx}, nil
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	sqlxtx, err := db.BeginTxx(ctx, opts)
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

type Tx struct {
	*sqlx.Tx
}

func (tx *Tx) Exec(ctx context.Context, b builder.Selecter) (sql.Result, error) {
	return Exec(ctx, tx, b)
}

func (tx *Tx) MustExec(ctx context.Context, b builder.Selecter) sql.Result {
	return MustExec(ctx, tx, b)
}

// type Conn struct {
// 	*sql.Conn
// }

// func (c *Conn) Select(b builder.Selecter, dest interface{}) error {
// 	return Select(c, b, dest)
// }
