package builder

import (
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	t.Run("SimpleOneByOne", func(t *testing.T) {
		expectedSql := "update table1 set a = $1, b = $2, c = $3, d = 'ddd'"
		b := Update("table1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Set("d = 'ddd'")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("SimpleAll", func(t *testing.T) {
		expectedSql := "update table1 set (a, b, c, d) = ($1, $2, $3, 'ddd')"
		b := Update("table1").
			Set("(a, b, c, d) = ($1, $2, $3, 'ddd')", 1, "bbb", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "update table1 set a = $1, b = $2, c = $3 where name = $4 and $5"
		b := Update("table1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Where("name = $2 and $1", "x", true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 5)
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "update table1 set a = $1, b = $2, c = $3 returning *"
			b := Update("table1").
				Set("a = $1", 1).
				Set("b = $1", "bbb").
				Set("c = $1", time.Now()).
				Returning("*")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 3)
		})

		t.Run("Columns", func(t *testing.T) {
			expectedSql := "update table1 set a = $1, b = $2, c = $3 returning id, name"
			b := Update("table1").
				Set("a = $1", 1).
				Set("b = $1", "bbb").
				Set("c = $1", time.Now()).
				Returning("id", "name")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 3)
		})
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "with table2 as (select id, name from table1 where name = $1) update table1 set a = $2, b = table2.name, c = $3 returning *"
		b := Update("table1").
			With("table2", Select("id", "name").
				From("table1").
				Where("name = $1", "d")).
			Set("a = $1", 1).
			Set("b = table2.name").
			Set("c = $1", time.Now()).
			Returning("*")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "update table1 as t1 set a = $1, b = $2, c = $3, d = t2.name from table2 as t2 where t1.id = t2.table1_id and t1.name = $4 and t2.name != $5 returning t1.id, t1.name"
		b := Update("table1 as t1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Set("d = t2.name").
			From("table2 as t2").
			Where("t1.id = t2.table1_id and t1.name = $2 and t2.name != $1", "x", "y").
			Returning("t1.id", "t1.name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 5)
	})
}
