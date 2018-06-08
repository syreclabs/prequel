package builder

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
	ForUpdate() Selecter
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
	Columns(expr ...string) Inserter
	Values(param ...interface{}) Inserter
	From(query Selecter) Inserter
	Returning(expr ...string) Inserter
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

func Update(table string) Updater {
	return &updater{table: table}
}

func Insert(table string) Inserter {
	return &inserter{into: table}
}

func Delete(table string) Deleter {
	return &deleter{from: table}
}
