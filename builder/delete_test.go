package builder

import (
	"testing"
)

func TestDelete(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "DELETE FROM table1"
		b := Delete("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "DELETE FROM table1 WHERE (cond1 = $1) AND (cond2 = $2 AND $3)"
		b := Delete("table1").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "DELETE FROM table1 RETURNING *"
			b := Delete("table1").
				Returning("*")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
				t.Error(err)
			}
		})

		t.Run("Columns", func(t *testing.T) {
			expectedSql := "DELETE FROM table1 RETURNING id, name"
			b := Delete("table1").
				Returning("id", "name")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
				t.Error(err)
			}
		})
	})

	t.Run("WithUsing", func(t *testing.T) {
		expectedSql := "DELETE FROM table1 USING table2 WHERE (table2.id = table1.id) AND (cond1 = $1) AND (cond2 = $2 AND $3)"
		b := Delete("table1").
			Using("table2").
			Where("table2.id = table1.id").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "DELETE FROM table1 USING table2 WHERE (table2.id = table1.id) AND (cond1 = $1) AND (cond2 = $2 AND $3) RETURNING id, name"
		b := Delete("table1").
			Using("table2").
			Where("table2.id = table1.id").
			Where("cond1 = $1", 1).
			Where("cond2 = $2 AND $1", true, 2).
			Returning("id", "name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "WITH table2 AS (SELECT id, name FROM table1 WHERE (name = $1)), table3 AS (SELECT id, name FROM table1 WHERE (name = $2)) DELETE FROM table1 WHERE (table1.id IN (SELECT id FROM table2)) AND (table1.id IN (SELECT id FROM table3)) AND (name = $3 AND $4) RETURNING id, name"
		b := Delete("table1").
			With("table2",
				Select("id", "name").
					From("table1").
					Where("name = $1", "d")).
			With("table3",
				Select("id", "name").
					From("table1").
					Where("name = $1", "d")).
			Where("table1.id IN (SELECT id FROM table2)").
			Where("table1.id IN (SELECT id FROM table3)").
			Where("name = $2 AND $1", "a", true).
			Returning("id", "name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 4); err != nil {
			t.Error(err)
		}
	})
}
