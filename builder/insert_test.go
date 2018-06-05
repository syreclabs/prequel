package builder

import (
	"testing"
	"time"
)

func TestSimpleInsert(t *testing.T) {
	expectedSql := "insert into table1 (a, b, c) values ($1, $2, $3)"
	b := Insert("table1").
		Columns("a", "b", "c").
		Values(1, "bbb", time.Now())

	sql, params, err := b.Build()
	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}

	if expectedSql != sql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
	}

	if len(params) != 3 {
		t.Errorf("expected params length to be (%d), got (%d)", 3, len(params))
	}

	// fmt.Printf("====> %#v\n", sql)
}
