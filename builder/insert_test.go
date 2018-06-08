package builder

import (
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3)"
		b := Insert("table1").
			Columns("a", "b", "c").
			Values(1, "bbb", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithoutColumns", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3)"
		b := Insert("table1").
			Values(1, "bbb", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 3)
	})

	t.Run("WithReturning", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) RETURNING *"
			b := Insert("table1").
				Values(1, "bbb", time.Now()).
				Returning("*")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 3)
		})

		t.Run("Columns", func(t *testing.T) {
			expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) RETURNING id, name"
			b := Insert("table1").
				Values(1, "bbb", time.Now()).
				Returning("id", "name")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 3)
		})
	})

	t.Run("MultipleRows", func(t *testing.T) {
		expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"
		b := Insert("table1").
			Values(1, "bbb", time.Now()).
			Values(2, "aaa", time.Now()).
			Values(3, "ccc", time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 9)
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "WITH table2 AS (SELECT id, name FROM table1 WHERE name = $1) INSERT INTO table1 SELECT * FROM table2 RETURNING *"
		b := Insert("table1").
			With("table2",
				Select("id", "name").
					From("table1").
					Where("name = $1", "d")).
			From(Select("*").From("table2")).
			Returning("*")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		validateGeneratedSql(t, sql, expectedSql, len(params), 1)
	})

	t.Run("OnConflict", func(t *testing.T) {
		t.Run("DoNothing", func(t *testing.T) {
			t.Run("Simple", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoNothing())

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 3)
			})

			t.Run("WithTagret", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT (a) DO NOTHING"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoNothing().Target("(a)"))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 3)
			})

			t.Run("WithConstraint", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT unique_a DO NOTHING"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoNothing().Target("ON CONSTRAINT unique_a"))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 3)
			})

			t.Run("WithWhere", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT WHERE a = $4 AND b = $5 DO NOTHING"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoNothing().
						Where("a = $1", []interface{}{"aaa"}).
						Where("b = $1", []interface{}{true}))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 5)
			})
		})

		t.Run("DoUpdate", func(t *testing.T) {
			t.Run("Simple", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT (a) DO UPDATE SET a = $4, b = $5, d = 'ddd', e = EXCLUDED.e"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoUpdate().Target("(a)").
						Update(Upsert().
							Set("a = $1", 1).
							Set("b = $1", "bbb").
							Set("d = 'ddd'").
							Set("e = EXCLUDED.e")))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 5)
			})

			t.Run("WithConstraint", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT unique_a DO UPDATE SET a = $4, b = $5, d = 'ddd', e = EXCLUDED.e"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoUpdate().Target("ON CONSTRAINT unique_a").
						Update(Upsert().
							Set("a = $1", 1).
							Set("b = $1", "bbb").
							Set("d = 'ddd'").
							Set("e = EXCLUDED.e")))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 5)
			})

			t.Run("WithWhere", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT (a) WHERE a = $4 AND b = $5 DO UPDATE SET a = $6, b = $7, d = 'ddd', e = EXCLUDED.e"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoUpdate().Target("(a)").
						Where("a = $1", []interface{}{"aaa"}).
						Where("b = $1", []interface{}{true}).
						Update(Upsert().
							Set("a = $1", 1).
							Set("b = $1", "bbb").
							Set("d = 'ddd'").
							Set("e = EXCLUDED.e")))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 7)
			})

			t.Run("WithUpdateWhere", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 VALUES ($1, $2, $3) ON CONFLICT (a) WHERE a = $4 AND b = $5 DO UPDATE SET a = $6, b = $7, d = 'ddd', e = EXCLUDED.e WHERE d = $8 AND c = 'ccc'"
				b := Insert("table1").
					Values(1, "bbb", time.Now()).
					OnConflict(DoUpdate().Target("(a)").
						Where("a = $1", []interface{}{"aaa"}).
						Where("b = $1", []interface{}{true}).
						Update(Upsert().
							Set("a = $1", 1).
							Set("b = $1", "bbb").
							Set("d = 'ddd'").
							Set("e = EXCLUDED.e").
							Where("d = $1", []interface{}{"aaa"}).
							Where("c = 'ccc'")))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 8)
			})

			t.Run("WithUpdateFromColumns", func(t *testing.T) {
				expectedSql := "INSERT INTO table1 (a, b, c) VALUES ($1, $2, $3) ON CONFLICT (a) WHERE a = $4 AND b = $5 DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b, c = EXCLUDED.c WHERE d = $6 AND c = 'ccc'"
				b := Insert("table1").
					Columns("a", "b", "c").
					Values(1, "bbb", time.Now()).
					OnConflict(DoUpdate().Target("(a)").
						Where("a = $1", []interface{}{"aaa"}).
						Where("b = $1", []interface{}{true}).
						Update(Upsert().
							SetExcluded("a", "b", "c").
							Where("d = $1", []interface{}{"aaa"}).
							Where("c = 'ccc'")))

				sql, params, err := b.Build()
				if err != nil {
					t.Fatalf("expected err to be nil, got %v", err)
				}

				validateGeneratedSql(t, sql, expectedSql, len(params), 6)
			})
		})
	})
}
