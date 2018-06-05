package builder

import (
	"testing"
)

func TestCondition(t *testing.T) {
	t.Run("Errors", func(t *testing.T) {
		t.Run("NegativePlaceholder", func(t *testing.T) {
			err := conds{&cond{"and", []interface{}{}}}.build(-2)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "negative index"
			if err.Error() != msg {
				t.Errorf("expected error (%s), got (%s)", msg, err.Error())
			}
		})

		t.Run("EmptyExpression", func(t *testing.T) {
			err := conds{&cond{"", []interface{}{}}}.build(0)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "empty expression"
			if err.Error() != msg {
				t.Errorf("expected error (%s), got (%s)", msg, err.Error())
			}
		})

		t.Run("MissingClosingQuote", func(t *testing.T) {
			err := conds{&cond{"where name = '' and ' and", []interface{}{}}}.build(0)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "missing closing quote"
			if err.Error() != msg {
				t.Errorf("expected error (%s), got (%s)", msg, err.Error())
			}
		})

		t.Run("InvalidPlaceholder", func(t *testing.T) {
			err := conds{&cond{"$ name = '' and $5 and true", []interface{}{}}}.build(0)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "invalid placeholder"
			if err.Error() != msg {
				t.Errorf("expected error (%s), got (%s)", msg, err.Error())
			}
		})

		t.Run("InvalidPlaceholderWithIndex", func(t *testing.T) {
			err := conds{&cond{"$3 name = '' and $5 and true", []interface{}{}}}.build(0)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "invalid placeholder index: 3"
			if err.Error() != msg {
				t.Errorf("expected error (%s), got (%s)", msg, err.Error())
			}
		})
	})

	t.Run("Conditions", func(t *testing.T) {
		expr := `where name='vasya \$1' and \$5 and $1 and $2`
		params := []interface{}{true, "b"}

		cc := conds{&cond{expr, params}, &cond{expr, params}, &cond{expr, params}}
		idx := 2
		err := cc.build(idx)
		if err != nil {
			t.Fatalf("expected error to be empty, got (%#v)", err)
		}

		for i, x := range cc {
			y := cond{expr, params}

			idx, err = y.build(idx)
			if err != nil {
				t.Fatalf("expected error to be empty, got (%#v)", err)
			}

			expectedIdx := 2 + ((i + 1) * len(params))
			if idx != expectedIdx {
				t.Errorf("expected idx to be (%d), got (%d)", expectedIdx, idx)
			}

			if x.expr != y.expr {
				t.Errorf("expected expression with idx (%d) to be (%s), got (%s)", i, y.expr, x.expr)
			}

			// fmt.Printf("====> %v pidx=%d\n", x.expr, idx)
		}
	})
}
