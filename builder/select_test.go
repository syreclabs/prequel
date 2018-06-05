package builder

import (
	"testing"
)

func TestSelect(t *testing.T) {

	t.Run("Simple", func(t *testing.T) {
		expectedSql := "select * from table1"
		b := Select("*").
			From("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithOrderBy", func(t *testing.T) {
		expectedSql := "select * from table1 order by b asc, a desc"
		b := Select("*").
			From("table1").
			OrderBy("b asc").
			OrderBy("a desc")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithLimit", func(t *testing.T) {
		expectedSql := "select * from table1 limit 10"
		b := Select("*").
			From("table1").
			Limit(10)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithOffset", func(t *testing.T) {
		expectedSql := "select * from table1 offset 5"
		b := Select("*").
			From("table1").
			Offset(5)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "select * from table1 where name = $1 and $2 and count > $3"
		b := Select("*").
			From("table1").
			Where("name = $2 and $1", "x", true).
			Where("count > $1", 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithDistinct", func(t *testing.T) {
		expectedSql := "select distinct on (a, b) a, b from table1"
		b := Select("a", "b").
			Distinct("a", "b").
			From("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithJoins", func(t *testing.T) {
		expectedSql := "select * from table1 inner join table2 on table2.a = table1.a left outer join table3 on table3.a = table1.a"
		b := Select("*").
			From("table1").
			From("inner join table2 on table2.a = table1.a").
			From("left outer join table3 on table3.a = table1.a")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithGroupBy", func(t *testing.T) {
		expectedSql := "select a, min(b) from table1 group by a"
		b := Select("a, min(b)").
			From("table1").
			GroupBy("a")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithHaving", func(t *testing.T) {
		expectedSql := "select a, min(b) from table1 where c = $1 group by a having max(c) > a and a < $2"
		b := Select("a, min(b)").
			From("table1").
			Where("c = $1", 1).
			GroupBy("a").
			Having("max(c) > a").
			Having("a < $1", 3)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 2)
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "with table2 as (select id, name from table1 where name = $1) select * from table1 where table2.id = table1.id and $2 and table2.name != 'bbb'"
		b := Select("*").
			With("table2", Select("id", "name").
				From("table1").
				Where("name = $1", "d")).
			From("table1").
			Where("table2.id = table1.id").
			Where("$1 and table2.name != 'bbb'", true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 2)
	})
}
