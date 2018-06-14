package builder

import (
	"testing"
)

func TestSelect(t *testing.T) {

	t.Run("Simple", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1"
		b := Select("*").
			From("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithOrderBy", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1 ORDER BY b asc, a desc"
		b := Select("*").
			From("table1").
			OrderBy("b asc").
			OrderBy("a desc")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithLimit", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1 LIMIT 10"
		b := Select("*").
			From("table1").
			Limit(10)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithOffset", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1 OFFSET 5"
		b := Select("*").
			From("table1").
			Offset(5)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithConditions", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1 WHERE (name = $1 AND $2) AND (count > $3)"
		b := Select("*").
			From("table1").
			Where("name = $2 AND $1", "x", true).
			Where("count > $1", 2)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 3); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithDistinct", func(t *testing.T) {
		expectedSql := "SELECT DISTINCT ON (a, b) a, b FROM table1"
		b := Select("a", "b").
			Distinct("a", "b").
			From("table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithJoins", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1 INNER JOIN table2 ON table2.a = table1.a LEFT OUTER JOIN table3 ON table3.a = table1.a"
		b := Select("*").
			From("table1").
			From("INNER JOIN table2 ON table2.a = table1.a").
			From("LEFT OUTER JOIN table3 ON table3.a = table1.a")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithGroupBy", func(t *testing.T) {
		expectedSql := "SELECT a, MIN(b) FROM table1 GROUP BY a"
		b := Select("a, MIN(b)").
			From("table1").
			GroupBy("a")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithHaving", func(t *testing.T) {
		expectedSql := "SELECT a, MIN(b) FROM table1 WHERE (c = $1) GROUP BY a HAVING MAX(c) > a AND a < $2"
		b := Select("a, MIN(b)").
			From("table1").
			Where("c = $1", 1).
			GroupBy("a").
			Having("MAX(c) > a").
			Having("a < $1", 3)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithQuery", func(t *testing.T) {
		expectedSql := "WITH table2 AS (SELECT id, name FROM table1 WHERE (name = $1)) SELECT * FROM table1 WHERE (table2.id = table1.id) AND ($2 AND table2.name != 'bbb')"
		b := Select("*").
			With("table2", Select("id", "name").
				From("table1").
				Where("name = $1", "d")).
			From("table1").
			Where("table2.id = table1.id").
			Where("$1 AND table2.name != 'bbb'", true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithUnions", func(t *testing.T) {
		expectedSql := "SELECT a, b FROM table1 UNION SELECT c as 'a', d as 'b' FROM table2 UNION ALL SELECT e as 'a', f as 'b' FROM table3 ORDER BY a"
		b := Select("a, b").
			From("table1").
			Union(false, Select("c as 'a', d as 'b'").From("table2")).
			Union(true, Select("e as 'a', f as 'b'").From("table3")).
			OrderBy("a")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})
}
