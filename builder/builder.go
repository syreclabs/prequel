package builder

import (
	"reflect"
)

// Builder interface is implemented by all specialized builders below and is used to
// generate SQL statements.
type Builder interface {
	// Builds returns generated SQL and parameters.
	Build() (string, []interface{}, error)
}

// Selecter is a SELECT statement builder.
type Selecter interface {
	Builder
	With(name string, q Builder) Selecter
	Columns(col string, params ...interface{}) Selecter
	From(from string) Selecter
	Where(where string, params ...interface{}) Selecter
	Union(all bool, q Selecter) Selecter
	Offset(offset uint64) Selecter
	Limit(limit uint64) Selecter
	Distinct(distinct ...string) Selecter
	GroupBy(groupBy string) Selecter
	Having(having string, params ...interface{}) Selecter
	OrderBy(orderBy string) Selecter
	For(locking string) Selecter
}

// Updater is an UPDATE statement builder.
type Updater interface {
	Builder
	With(name string, q Builder) Updater
	From(from string) Updater
	Set(set string, params ...interface{}) Updater
	Where(where string, params ...interface{}) Updater
	Returning(returning ...string) Updater
}

// Inserter is an INSERT statement builder.
type Inserter interface {
	Builder
	With(name string, q Builder) Inserter
	Columns(col ...string) Inserter
	Values(params ...interface{}) Inserter
	From(q Selecter) Inserter
	OnConflictDoNothing(target string, params ...interface{}) Inserter
	Returning(returning ...string) Inserter
}

// Upserter is an INSERT statement builder.
type Upserter interface {
	Builder
	With(name string, q Builder) Upserter
	Columns(col ...string) Upserter
	Values(params ...interface{}) Upserter
	From(q Selecter) Upserter
	Update(update string, params ...interface{}) Upserter // unless specified, Columns with EXCLUDED values used
	Returning(returning ...string) Upserter
}

// Deleter is a DELETE statement builder.
type Deleter interface {
	Builder
	With(name string, q Builder) Deleter
	Using(using string) Deleter
	Where(where string, params ...interface{}) Deleter
	Returning(returning ...string) Deleter
}

func Select(col ...string) Selecter {
	s := &selecter{}
	for _, c := range col {
		s.columns = append(s.columns, &expr{c, nil})
	}
	return s
}

func Insert(table string) Inserter {
	return &inserter{into: table, onConflictDoNothing: false}
}

func Update(table string) Updater {
	return &updater{table: table}
}

func Upsert(table, target string, params ...interface{}) Upserter {
	return &upserter{into: table, onConflictTarget: &expr{target, params}}
}

func Delete(table string) Deleter {
	return &deleter{from: table}
}

func SQL(query string, params ...interface{}) Builder {
	return &sqler{query: expr{text: query, params: params}}
}

type DefaultValue struct{}

func (dv DefaultValue) String() string {
	return "<DEFAULT>"
}

func Default(value interface{}) interface{} {
	val := reflect.ValueOf(value)
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		if val.Int() == 0 {
			return DefaultValue{}
		}
	case uint, uint8, uint16, uint32, uint64:
		if val.Uint() == 0 {
			return DefaultValue{}
		}
	case float32, float64:
		if val.Float() == 0 {
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
		case reflect.Slice, reflect.Map:
			if reflect.ValueOf(v).Len() == 0 {
				return DefaultValue{}
			}
		}
	}

	return value
}
