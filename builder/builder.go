package builder

type Builder interface {
	Build() (string, []interface{}, error)
}

type Selecter interface {
	From(from string) Selecter
	Where(cond string, param ...interface{}) Selecter
	Offset(offset int) Selecter
	Limit(limit int) Selecter
	Distinct(expr ...string) Selecter
	GroupBy(expr string) Selecter
	Having(cond string, param ...interface{}) Selecter
	Builder
}

type Updater interface {
	From(from string) Updater
	Columns(expr ...string) Updater
	Values(param ...interface{}) Updater
	Where(cond string, param ...interface{}) Updater
	Returning(expr ...string) Updater
	Builder
}

type Inserter interface {
	Columns(expr ...string) Inserter
	Values(param ...interface{}) Inserter
	Builder
}

type Deleter interface {
	Where(cond string, param ...interface{}) Deleter
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
