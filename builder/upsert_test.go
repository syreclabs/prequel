package builder

import (
	"testing"
	"time"
)

func TestUpsert(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c"
		b := Upsert("table1", "(a)").
			Columns("a", "b", "c").
			Values(1, "bbb", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithDefaultFirstParam", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 (a, b, c) VALUES (DEFAULT, $1, $2) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c"
		b := Upsert("table1", "(a)").
			Columns("a", "b", "c").
			Values(Default(0), "bbb", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithDefaultMidParam", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3), (DEFAULT, $4, $5), ($6, $7, $8) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c"
		b := Upsert("table1", "(a)").
			Columns("a", "b", "c").
			Values(1, "bbb", time.Now()).
			Values(Default(0), "aaa", time.Now()).
			Values(3, "ccc", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 8); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c RETURNING *"
			b := Upsert("table1", "(a)").
				Columns("a", "b", "c").
				Values(1, "bbb", time.Now()).
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
			expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c RETURNING id, name"
			b := Upsert("table1", "(a)").
				Columns("a", "b", "c").
				Values(1, "bbb", time.Now()).
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

	t.Run("MultipleRows", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9) ON CONFLICT ON CONSTRAINT table1_a_uniq DO UPDATE SET name = EXCLUDED.name WHERE name != $10"
		b := Upsert("table1", "ON CONSTRAINT table1_a_uniq").
			Values(1, "bbb", time.Now()).
			Values(2, "aaa", time.Now()).
			Values(3, "ccc", time.Now()).
			Update("name = EXCLUDED.name WHERE name != $1", "none")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 10); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "WITH table2 AS (SELECT id, name FROM table1 WHERE (name = $1)) INSERT INTO table1 SELECT * FROM table2 ON CONFLICT (a) WHERE a != $2 DO UPDATE SET a = EXCLUDED.a WHERE name != table2.name RETURNING *"
		b := Upsert("table1", "(a) WHERE a != $1", "ddd").
			With("table2",
				Select("id", "name").
					From("table1").
					Where("name = $1", "d")).
			From(Select("*").From("table2")).
			Update("a = EXCLUDED.a WHERE name != table2.name").
			Returning("*")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})
}
