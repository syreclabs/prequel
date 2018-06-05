package builder

import (
	"testing"
)

func TestSelect(t *testing.T) {

	t.Run("Simple", func(t *testing.T) {
		expectedSql := "select distinct on (a, b) a, b, c as d from table1 inner join table2 on table1.a = table2.b where cond1 = $1 and cond2 = $2 offset 5 limit 10"
		b := Select("a", "b", "c as d").
			From("table1").
			From("inner join table2 on table1.a = table2.b").
			Where("cond1 = $1", 1).
			Where("cond2 = $1", 2).
			Distinct("a", "b").
			Offset(5).
			Limit(10)

		sql, params, err := b.Build()
		if err != nil {
			t.Errorf("expected err to be nil, got %v", err)
		}

		if expectedSql != sql {
			t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
		}

		if len(params) != 2 {
			t.Errorf("expected params length to be (%d), got (%d)", 2, len(params))
		}

		// fmt.Printf("====> %#v\n", sql)
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "select a, MIN(b), MAX(c) as d from table1 inner join table2 on table1.a = table2.b where cond1 = $1 and cond2 = $3 AND $2 group by a having MAX(c) > a and a < $4 offset 5 limit 10"
		b := Select("a", "MIN(b)", "MAX(c) as d").
			From("table1").
			From("inner join table2 on table1.a = table2.b").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2).
			GroupBy("a").
			Having("MAX(c) > a").
			Having("a < $1", 3).
			Offset(5).
			Limit(10)

		sql, params, err := b.Build()
		if err != nil {
			t.Errorf("expected err to be nil, got %v", err)
		}

		if expectedSql != sql {
			t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
		}

		if len(params) != 4 {
			t.Errorf("expected params length to be (%d), got (%d)", 4, len(params))
		}

		// fmt.Printf("====> %#v\n", sql)
	})
}
