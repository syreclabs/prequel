package builder

import (
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "update table1 set a = $1, b = $2, c = $3 where name = $5 and $4"
		b := Update("table1").
			Columns("a", "b", "c").
			Values(1, "bbb", time.Now()).
			Where("name = $2 and $1", "x", true)

		sql, params, err := b.Build()
		if err != nil {
			t.Errorf("expected err to be nil, got %v", err)
		}

		if expectedSql != sql {
			t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
		}

		if len(params) != 5 {
			t.Errorf("expected params length to be (%d), got (%d)", 5, len(params))
		}

		// fmt.Printf("====> %#v\n", sql)
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "update table1 as t1 set a = $1, b = $2, c = $3 from table2 as t2 where t1.id = t2.table1_id and t1.name = $5 and t2.name != $4 returning t1.id, t1.name"
		b := Update("table1 as t1").
			Columns("a", "b", "c").
			Values(1, "bbb", time.Now()).
			From("table2 as t2").
			Where("t1.id = t2.table1_id and t1.name = $2 and t2.name != $1", "x", "y").
			Returning("t1.id", "t1.name")

		sql, params, err := b.Build()
		if err != nil {
			t.Errorf("expected err to be nil, got %v", err)
		}

		if expectedSql != sql {
			t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
		}

		if len(params) != 5 {
			t.Errorf("expected params length to be (%d), got (%d)", 5, len(params))
		}

		// fmt.Printf("====> %#v\n", sql)
	})
}
