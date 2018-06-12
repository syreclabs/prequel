package builder

import (
	"reflect"
)

// TODO: support DEFAULT as value for insert/update for column

// Builder interface is implemented by all specialized builders below and is used to
// generate SQL statements.
type Builder interface {
	// Builds returns generated SQL and parameters.
	Build() (string, []interface{}, error)
}

// Selecter is a SELECT statement builder.
type Selecter interface {
	Builder
	With(name string, query Selecter) Selecter
	From(from string) Selecter
	Where(cond string, param ...interface{}) Selecter
	Offset(offset uint64) Selecter
	Limit(limit uint64) Selecter
	Distinct(expr ...string) Selecter
	GroupBy(expr string) Selecter
	Having(cond string, param ...interface{}) Selecter
	OrderBy(expr string) Selecter
	For(locking string) Selecter
}

// Updater is an UPDATE statement builder.
type Updater interface {
	Builder
	With(name string, query Selecter) Updater
	From(from string) Updater
	Set(expr string, param ...interface{}) Updater
	Where(cond string, param ...interface{}) Updater
	Returning(expr ...string) Updater
}

// Inserter is an INSERT statement builder.
type Inserter interface {
	Builder
	With(name string, query Selecter) Inserter
	Columns(cols ...string) Inserter
	Values(param ...interface{}) Inserter
	From(query Selecter) Inserter
	OnConflictDoNothing(target string, params ...interface{}) Inserter
	Returning(expr ...string) Inserter
}

// Upserter is an INSERT statement builder.
type Upserter interface {
	Builder
	With(name string, query Selecter) Upserter
	Columns(cols ...string) Upserter
	Values(param ...interface{}) Upserter
	From(query Selecter) Upserter
	Update(update string, param ...interface{}) Upserter // unless specified, Columns with EXCLUDED values used
	Returning(expr ...string) Upserter
}

// Deleter is a DELETE statement builder.
type Deleter interface {
	Builder
	With(name string, query Selecter) Deleter
	Using(using string) Deleter
	Where(cond string, param ...interface{}) Deleter
	Returning(expr ...string) Deleter
}

func Select(expr ...string) Selecter {
	return &selecter{expr: expr}
}

func Insert(table string) Inserter {
	return &inserter{into: table, onConflictDoNothing: false}
}

func Update(table string) Updater {
	return &updater{table: table}
}

func Upsert(table, target string, param ...interface{}) Upserter {
	return &upserter{into: table, onConflictTarget: &cond{target, param}}
}

func Delete(table string) Deleter {
	return &deleter{from: table}
}

type DefaultValue struct{}

func (dv DefaultValue) String() string {
	return "<DEFAULT>"
}

// func (dv DefaultValue) Value() (driver.Value, error) {
// 	return nil, nil
// }

func Default(value interface{}) interface{} {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		if v == 0 {
			return DefaultValue{}
		}
	case string:
		if v == "" {
			return DefaultValue{}
		}
	default:
		switch reflect.TypeOf(v).Kind() {
		case reflect.Ptr:
			if reflect.ValueOf(v).IsNil() {
				return DefaultValue{}
			}
		case reflect.Slice, reflect.Map, reflect.Array:
			if reflect.ValueOf(v).Len() == 0 {
				return DefaultValue{}
			}
		}
	}

	return value
}
