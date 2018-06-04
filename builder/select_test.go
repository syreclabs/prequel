package builder

import (
	"fmt"
	"testing"
)

func TestSimpleSelect(t *testing.T) {
	b := Select("a", "b", "c as d").
		From("table1").
		From("inner join table2 on table1.a = table2.b").
		Where("cond1 = $1", 1).
		Where("cond2 = $2", 2).
		Distinct("a", "b").
		Offset(5).
		Limit(10)

	sql, _, err := b.Build()

	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}
	fmt.Printf("====> %#v\n", sql)
}
