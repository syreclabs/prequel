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
	Builder
}

// type Updater interface {
// 	Builder
// }

// type Inserter interface {
// 	Builder
// }

// type Deleter interface {
// 	Builder
// }

func Select(expr ...string) Selecter {
	return &selecter{expr: expr}
}

// Update(table string) UpdateBuilder
// Insert(table string) InsertBuilder
// Delete(table string) DeleteBuilder
