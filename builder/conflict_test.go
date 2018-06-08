package builder

import (
	"testing"
)

func TestConflict(t *testing.T) {
	t.Run("DoNothing", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			expectedSql := "ON CONFLICT DO NOTHING"
			b := DoNothing()

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 0)
		})

		t.Run("WithTagret", func(t *testing.T) {
			expectedSql := "ON CONFLICT (a) DO NOTHING"
			b := DoNothing().Target("(a)")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 0)
		})

		t.Run("WithConstraint", func(t *testing.T) {
			expectedSql := "ON CONFLICT ON CONSTRAINT unique_a DO NOTHING"
			b := DoNothing().Target("ON CONSTRAINT unique_a")

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 0)
		})

		t.Run("WithWhere", func(t *testing.T) {
			expectedSql := "ON CONFLICT WHERE a = $1 AND b = $2 DO NOTHING"
			b := DoNothing().
				Where("a = $1", []interface{}{"aaa"}).
				Where("b = $1", []interface{}{true})

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 2)
		})
	})

	t.Run("DoUpdate", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			expectedSql := "ON CONFLICT (a) DO UPDATE SET a = $1, b = $2, d = 'ddd', e = EXCLUDED.e"
			b := DoUpdate().Target("(a)").
				Update(Upsert().
					Set("a = $1", 1).
					Set("b = $1", "bbb").
					Set("d = 'ddd'").
					Set("e = EXCLUDED.e"))

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 2)
		})

		t.Run("WithTagret", func(t *testing.T) {
			expectedSql := "ON CONFLICT (a) DO UPDATE SET a = $1, b = $2, d = 'ddd', e = EXCLUDED.e"
			b := DoUpdate().Target("(a)").
				Update(Upsert().
					Set("a = $1", 1).
					Set("b = $1", "bbb").
					Set("d = 'ddd'").
					Set("e = EXCLUDED.e"))

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 2)
		})

		t.Run("WithConstraint", func(t *testing.T) {
			expectedSql := "ON CONFLICT ON CONSTRAINT unique_a DO UPDATE SET a = $1, b = $2, d = 'ddd', e = EXCLUDED.e"
			b := DoUpdate().Target("ON CONSTRAINT unique_a").
				Update(Upsert().
					Set("a = $1", 1).
					Set("b = $1", "bbb").
					Set("d = 'ddd'").
					Set("e = EXCLUDED.e"))

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 2)
		})

		t.Run("WithWhere", func(t *testing.T) {
			expectedSql := "ON CONFLICT (a) WHERE a = $1 AND b = $2 DO UPDATE SET a = $3, b = $4, d = 'ddd', e = EXCLUDED.e"
			b := DoUpdate().Target("(a)").
				Where("a = $1", []interface{}{"aaa"}).
				Where("b = $1", []interface{}{true}).
				Update(Upsert().
					Set("a = $1", 1).
					Set("b = $1", "bbb").
					Set("d = 'ddd'").
					Set("e = EXCLUDED.e"))

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 4)
		})

		t.Run("WithUpdateWhere", func(t *testing.T) {
			expectedSql := "ON CONFLICT (a) WHERE a = $1 AND b = $2 DO UPDATE SET a = $3, b = $4, d = 'ddd', e = EXCLUDED.e WHERE d = $5 AND c = 'ccc'"
			b := DoUpdate().Target("(a)").
				Where("a = $1", []interface{}{"aaa"}).
				Where("b = $1", []interface{}{true}).
				Update(Upsert().
					Set("a = $1", 1).
					Set("b = $1", "bbb").
					Set("d = 'ddd'").
					Set("e = EXCLUDED.e").
					Where("d = $1", []interface{}{"aaa"}).
					Where("c = 'ccc'"))

			sql, params, err := b.Build()
			if err != nil {
				t.Fatalf("expected err to be nil, got %v", err)
			}

			validateGeneratedSql(t, sql, expectedSql, len(params), 5)
		})
	})
}
