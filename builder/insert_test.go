package builder

import (
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "insert into table1 (a, b, c) values ($1, $2, $3)"
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
		expectedSql := "insert into table1 values ($1, $2, $3)"
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
			expectedSql := "insert into table1 values ($1, $2, $3) returning *"
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
			expectedSql := "insert into table1 values ($1, $2, $3) returning id, name"
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
		expectedSql := "insert into table1 values ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"
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
		expectedSql := "with table2 as (select id, name from table1 where name = $1) insert into table1 select * from table2 returning *"
		b := Insert("table1").
			With("table2", Select("id", "name").
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
}
