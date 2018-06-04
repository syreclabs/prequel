package prequel

import (
	"database/sql"

	"github.com/dmgk/prequel/builder"
	"github.com/jmoiron/sqlx"
)

type Queryer interface {
}

type Execer interface {
}

func Select(q Queryer, b builder.Selecter, dest interface{}) error {
	sql, params, err := b.Build()
	if err != nil {
		return err
	}
	return sqlx.Select(q, dest, sql, params)
}

type DB struct {
	*sqlx.DB
}

func (db *DB) Select(b builder.Selecter, dest interface{}) error {
	return Select(db, b, dest)
}

type Conn struct {
	*sql.Conn
}

func (c *Conn) Select(b builder.Selecter, dest interface{}) error {
	return Select(c, b, dest)
}
