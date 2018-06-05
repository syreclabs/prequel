package builder

import (
	"testing"
)

func validateGeneratedSql(t *testing.T, generatedSql, expectedSql string, generatedParamsCound, expectedParamsCount int) {
	if expectedSql != generatedSql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, generatedSql)
	}

	if generatedParamsCound != expectedParamsCount {
		t.Errorf("expected params length to be (%d), got (%d)", expectedParamsCount, generatedParamsCound)
	}
}

func TestDelete(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "delete from table1"
		b := Delete("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 0)
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "delete from table1 where cond1 = $1 and cond2 = $2 AND $3"
		b := Delete("table1").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "delete from table1 returning *"
			b := Delete("table1").
				Returning("*")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 0)
		})

		t.Run("Columns", func(t *testing.T) {
			expectedSql := "delete from table1 returning id, name"
			b := Delete("table1").
				Returning("id", "name")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 0)
		})
	})

	t.Run("WithUsing", func(t *testing.T) {
		expectedSql := "delete from table1 using table2 where table2.id = table1.id and cond1 = $1 and cond2 = $2 AND $3"
		b := Delete("table1").
			Using("table2").
			Where("table2.id = table1.id").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "delete from table1 using table2 where table2.id = table1.id and cond1 = $1 and cond2 = $2 and $3 returning id, name"
		b := Delete("table1").
			Using("table2").
			Where("table2.id = table1.id").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 and $1", true, 2).
			Returning("id", "name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "with table2 as (select id, name from table1 where name = $1) delete from table1 where table2.id = table1.id and name = $2 and $3 returning id, name"
		b := Delete("table1").
			With("table2", Select("id", "name").
				From("table1").
				Where("name = $1", "d")).
			Where("table2.id = table1.id").
			Where("name = $2 and $1", "a", true).
			Returning("id", "name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})
}
