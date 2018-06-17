package builder

import (
	"testing"
	"time"
)

func TestInsect(t *testing.T) {

	t.Run("Simple", func(t *testing.T) {
		expectedSql := "WITH sel AS (SELECT * FROM table), ins AS (INSERT INTO table (a, b, c) SELECT $1, DEFAULT, $2 WHERE (NOT EXISTS(SELECT * FROM sel)) RETURNING *) SELECT * FROM ins UNION ALL SELECT * FROM sel"
		b := Insect("table").
			Columns("a", "b", "c").
			Values(1, Default(0), time.Now())

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithReturning", func(t *testing.T) {
		expectedSql := "WITH sel AS (SELECT d, f FROM table), ins AS (INSERT INTO table (a, b, c) SELECT $1, DEFAULT, $2 WHERE (NOT EXISTS(SELECT * FROM sel)) RETURNING d, f) SELECT d, f FROM ins UNION ALL SELECT d, f FROM sel"
		b := Insect("table").
			Columns("a", "b", "c").
			Values(1, Default(0), time.Now()).
			Returning("d", "f")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithWhere", func(t *testing.T) {
		expectedSql := "WITH sel AS (SELECT * FROM table WHERE (d = $1) AND (f = $2)), ins AS (INSERT INTO table (a, b, c) SELECT $3, DEFAULT, $4 WHERE (NOT EXISTS(SELECT * FROM sel)) RETURNING *) SELECT * FROM ins UNION ALL SELECT * FROM sel"
		b := Insect("table").
			Columns("a", "b", "c").
			Values(1, Default(0), time.Now()).
			Where("d = $1", "aaa").
			Where("f = $1", "bbb")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 4); err != nil {
			t.Error(err)
		}
	})
}
