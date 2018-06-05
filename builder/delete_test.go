package builder

import (
	"testing"
)

func TestSimpleDelete(t *testing.T) {
	expectedSql := "delete from table1 where cond1 = $1 and cond2 = $3 AND $2"

	b := Delete("table1").
		Where("cond1 = $1", 1).
		Where("cond2 = $2 AND $1", true, 2)

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
