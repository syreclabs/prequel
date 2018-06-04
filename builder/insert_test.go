package builder

import (
	"fmt"
	"testing"
	"time"
)

func TestSimpleInsert(t *testing.T) {
	b := Insert("table1").
		Columns("a", "b", "c").
		Values(1, "bbb", time.Now())

	sql, params, err := b.Build()

	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}
	fmt.Printf("====> %#v\n", sql)

	expectedSql := "insert into table1 (a, b, c) values ($1, $2, $3)"
	if expectedSql != sql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
	}

	fmt.Printf("====> %#v\n", params)
}
