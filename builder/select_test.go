package builder

import (
	"fmt"
	"testing"
)

func TestSimpleSelect(t *testing.T) {
	b := Select("a", "b", "c as d").
		From("table1").
		From("inner join table2 on table1.a = table2.b").
		Where("cond1 = $1", 1).
		Where("cond2 = $2", 2).
		Distinct("a", "b").
		Offset(5).
		Limit(10)

	sql, _, err := b.Build()

	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}
	fmt.Printf("====> %#v\n", sql)

	expectedSql := "select distinct on (a, b) a, b, c as d from table1 inner join table2 on table1.a = table2.b where cond1 = $1 and cond2 = $2 offset 5 limit 10"
	if expectedSql != sql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
	}
}

func TestComplexSelect(t *testing.T) {
	b := Select("a", "MIN(b)", "MAX(c) as d").
		From("table1").
		From("inner join table2 on table1.a = table2.b").
		Where("cond1 = $1", 1).
		Where("cond2 = $2", 2).
		GroupBy("a").
		Having("MAX(c) > a").
		Offset(5).
		Limit(10)

	sql, _, err := b.Build()

	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}

	fmt.Printf("====> %#v\n", sql)

	expectedSql := "select a, MIN(b), MAX(c) as d from table1 inner join table2 on table1.a = table2.b where cond1 = $1 and cond2 = $2 group by a having MAX(c) > a offset 5 limit 10"
	if expectedSql != sql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
	}
}
