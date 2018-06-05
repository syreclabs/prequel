package builder

// TODO: support DEFAULT as value for insert/update for column

type Builder interface {
	Build() (string, []interface{}, error)
}

type Selecter interface {
	With(name string, query Selecter) Selecter
	From(from string) Selecter
	Where(cond string, param ...interface{}) Selecter
	Offset(offset int) Selecter
	Limit(limit int) Selecter
	Distinct(expr ...string) Selecter
	GroupBy(expr string) Selecter
	Having(cond string, param ...interface{}) Selecter
	OrderBy(expr string) Selecter
	Builder
}

type Updater interface {
	With(name string, query Selecter) Updater
	From(from string) Updater
	Set(expr string, param ...interface{}) Updater
	Where(cond string, param ...interface{}) Updater
	Returning(expr ...string) Updater
	Builder
}

type Inserter interface {
	With(name string, query Selecter) Inserter
	Columns(expr ...string) Inserter
	Values(param ...interface{}) Inserter
	From(query Selecter) Inserter
	Returning(expr ...string) Inserter
	Builder
}

type Deleter interface {
	With(name string, query Selecter) Deleter
	Using(using string) Deleter
	Where(cond string, param ...interface{}) Deleter
	Returning(expr ...string) Deleter
	Builder
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
