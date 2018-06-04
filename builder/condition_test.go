package builder

import (
	"fmt"
	"testing"
)

func TestCondition(t *testing.T) {
	c := cond{`where name='vasya \$1' and \$5 and $1 and $2`, []interface{}{"a", "b"}}

	pidx, err := c.build(10)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("====> %v pidx=%d\n", c.expr, pidx)
}
