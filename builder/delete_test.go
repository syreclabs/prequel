package builder

import (
	"fmt"
	"testing"
)

func TestSimpleDelete(t *testing.T) {
	b := Delete("table1").
		Where("cond1 = $1", 1).
		Where("cond2 = $2", 2)

	sql, params, err := b.Build()

	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}
	fmt.Printf("====> %#v\n", sql)

	expectedSql := "delete from table1 where cond1 = $1 and cond2 = $2"
	if expectedSql != sql {
		t.Errorf("expected sql (%s), got (%s)", expectedSql, sql)
	}

	fmt.Printf("====> %#v\n", params)
}
