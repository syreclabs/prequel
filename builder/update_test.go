package builder

import (
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	t.Run("SimpleOneByOne", func(t *testing.T) {
		expectedSql := "UPDATE table1 SET a = $1, b = $2, c = $3, d = 'ddd'"
		b := Update("table1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Set("d = 'ddd'")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("SimpleAll", func(t *testing.T) {
		expectedSql := "UPDATE table1 SET (a, b, c, d, r) = ($1, $2, $3, 'ddd', $4)"
		b := Update("table1").
			Set("(a, b, c, d, r) = ($1, $2, $3, 'ddd', $4)", 1, "bbb", time.Now(), true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 4); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "UPDATE table1 SET a = $1, b = $2, c = $3 WHERE (name = $4 AND $5)"
		b := Update("table1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Where("name = $2 AND $1", "x", true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 5); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "UPDATE table1 SET a = $1, b = $2, c = $3 RETURNING *"
			b := Update("table1").
				Set("a = $1", 1).
				Set("b = $1", "bbb").
				Set("c = $1", time.Now()).
				Returning("*")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
				t.Error(err)
			}
		})

		t.Run("Columns", func(t *testing.T) {
			expectedSql := "UPDATE table1 SET a = $1, b = $2, c = $3 RETURNING id, name"
			b := Update("table1").
				Set("a = $1", 1).
				Set("b = $1", "bbb").
				Set("c = $1", time.Now()).
				Returning("id", "name")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
				t.Error(err)
			}
		})
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "WITH table2 AS (SELECT id, name FROM table1 WHERE (name = $1)) UPDATE table1 SET a = $2, b = table2.name, c = $3 FROM table2 RETURNING *"
		b := Update("table1").
			With("table2", Select("id", "name").
				From("table1").
				Where("name = $1", "d")).
			Set("a = $1", 1).
			Set("b = table2.name").
			Set("c = $1", time.Now()).
			From("table2").
			Returning("*")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithParamsInFrom", func(t *testing.T) {
		expectedSql := "UPDATE table1 AS t1 SET a = t2.a, b = $1, c = t3.a FROM table2 AS t2 INNER JOIN table3 AS t3 ON t3.id = t2.id AND t3.name = $2 RETURNING *"
		b := Update("table1 AS t1").
			Set("a = t2.a").
			Set("b = $1", 2).
			Set("c = t3.a").
			From("table2 AS t2").
			From("INNER JOIN table3 AS t3 ON t3.id = t2.id AND t3.name = $1", "test").
			Returning("*")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("Complex", func(t *testing.T) {
		expectedSql := "UPDATE table1 AS t1 SET a = $1, b = $2, c = $3, d = t2.name FROM table2 AS t2 WHERE (t1.id = t2.table1_id AND t1.name = $4 AND t2.name != $5) RETURNING t1.id, t1.name"
		b := Update("table1 AS t1").
			Set("a = $1", 1).
			Set("b = $1", "bbb").
			Set("c = $1", time.Now()).
			Set("d = t2.name").
			From("table2 AS t2").
			Where("t1.id = t2.table1_id AND t1.name = $2 AND t2.name != $1", "x", "y").
			Returning("t1.id", "t1.name")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 5); err != nil {
			t.Error(err)
		}
	})
}
