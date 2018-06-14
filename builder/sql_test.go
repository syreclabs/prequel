package builder

import "testing"

func TestSQL(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		expectedSql := "SELECT * FROM table1"
		b := SQL("SELECT * FROM table1")

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 0); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithParams", func(t *testing.T) {
		expectedSql := "SELECT id, name FROM table1 WHERE id = $1 AND $2"
		b := SQL("SELECT id, name FROM table1 WHERE id = $1 AND $2", 1, true)

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 2); err != nil {
			t.Error(err)
		}
	})

	t.Run("WithIN", func(t *testing.T) {
		expectedSql := "SELECT id, name FROM table1 WHERE $1 AND id IN ($2,$3,$4)"
		b := SQL("SELECT id, name FROM table1 WHERE $1 AND id IN ($2)", true, []int{2, 3, 4})

		sql, params, err := b.Build()
		if err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		if err := validateBuilderResult(sql, expectedSql, len(params), 4); err != nil {
			t.Error(err)
		}
	})
}
