package builder

import (
	"reflect"
	"testing"
)

func TestCondition(t *testing.T) {
	t.Run("Errors", func(t *testing.T) {
		t.Run("NegativePlaceholder", func(t *testing.T) {
			err := exprs{&expr{"and", []interface{}{}}}.build(0)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "start index should be >= 1"
			if err.Error() != msg {
				t.Errorf("expected error %q, got %q", msg, err.Error())
			}
		})

		t.Run("EmptyExpression", func(t *testing.T) {
			err := exprs{&expr{"", []interface{}{}}}.build(1)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "empty expression"
			if err.Error() != msg {
				t.Errorf("expected error %q, got %q", msg, err.Error())
			}
		})

		t.Run("MissingClosingQuote", func(t *testing.T) {
			err := exprs{&expr{"name = '' and ' and", []interface{}{}}}.build(1)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "missing closing quote"
			if err.Error() != msg {
				t.Errorf("expected error %q, got %q", msg, err.Error())
			}
		})

		t.Run("InvalidPlaceholder", func(t *testing.T) {
			err := exprs{&expr{"$ name = '' and $5 and true", []interface{}{}}}.build(1)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "invalid placeholder"
			if err.Error() != msg {
				t.Errorf("expected error %q, got %q", msg, err.Error())
			}
		})

		t.Run("InvalidPlaceholderWithIndex", func(t *testing.T) {
			err := exprs{&expr{"$3 name = '' and $5 and true", []interface{}{}}}.build(1)
			if err == nil {
				t.Fatal("expected error not to be empty")
			}

			msg := "invalid placeholder index: 3"
			if err.Error() != msg {
				t.Errorf("expected error %q, got %q", msg, err.Error())
			}
		})
	})

	t.Run("Conditions", func(t *testing.T) {
		examples := []struct {
			startIdx       int
			cond           expr
			expected       expr
			expectedError  string
			expectedNewIdx int
		}{
			{
				1,
				expr{"name = 'user'", []interface{}{}},
				expr{"name = 'user'", []interface{}{}},
				"",
				1,
			},
		}

		for i, x := range examples {
			newIdx, err := x.cond.build(x.startIdx)
			if x.expectedError == "" {
				if err != nil {
					t.Fatalf("example %d: expected error to be nil, got %#v", i, err)
				}
				if x.cond.text != x.expected.text {
					t.Errorf("example %d: expected text to be %q, got %q", i, x.expected.text, x.cond.text)
				}
				if len(x.expected.params) > 0 && !reflect.DeepEqual(x.cond.params, x.expected.params) {
					t.Errorf("example %d: expected params to be %v, got %v", i, x.expected.params, x.cond.params)
				}
				if newIdx != x.expectedNewIdx {
					t.Errorf("example %d: expected newIdx to be %d, got %d", i, x.expectedNewIdx, newIdx)
				}
			} else if x.expectedError != "" && x.expectedError != err.Error() {
				t.Fatalf("example %d: expected error to be %q, got %q", i, x.expectedError, err.Error())
			}
		}
	})

	t.Run("In", func(t *testing.T) {
		examples := []struct {
			startIdx       int
			cond           expr
			expected       expr
			expectedError  string
			expectedNewIdx int
		}{
			{
				3,
				expr{"id IN ($1)", []interface{}{[]int{1, 2, 3}}},
				expr{"id IN ($3,$4,$5)", []interface{}{1, 2, 3}},
				"",
				6,
			},
			{
				5,
				expr{"name=$1 AND id IN ($2)", []interface{}{"name", []int{1, 2, 3}}},
				expr{"name=$5 AND id IN ($6,$7,$8)", []interface{}{"name", 1, 2, 3}},
				"",
				9,
			},
			{
				7,
				expr{"name=$1 AND age=$3 AND last_name=$4 AND id IN ($2)", []interface{}{"name", []int{1, 2, 3}, 42, "last"}},
				expr{"name=$7 AND age=$8 AND last_name=$9 AND id IN ($10,$11,$12)", []interface{}{"name", 42, "last", 1, 2, 3}},
				"",
				13,
			},
			{
				9,
				expr{"hash=$2 AND names IN ($1)", []interface{}{[]int{1, 2}, []byte("bytes")}},
				expr{"hash=$9 AND names IN ($10,$11)", []interface{}{[]byte("bytes"), 1, 2}},
				"",
				12,
			},
			{
				1,
				expr{"id IN ($1) AND other_id IN ($2)", []interface{}{[]int{1, 2, 3}, []int{4, 5}}},
				expr{"id IN ($1,$2,$3) AND other_id IN ($4,$5)", []interface{}{1, 2, 3, 4, 5}},
				"",
				6,
			},
			{
				1,
				expr{"id IN ($1) AND other_id IN ($2)", []interface{}{[]int{1, 2}, []int{3}}},
				expr{"id IN ($1,$2) AND other_id IN ($3)", []interface{}{1, 2, 3}},
				"",
				4,
			},
			{
				1,
				expr{"name=$1 AND id IN ($2)", []interface{}{"name", []int{}}},
				expr{},
				"empty slice passed as 'IN' parameter",
				3,
			},
			{
				1,
				expr{"id IN ($1) AND other_id IN ($2)", []interface{}{[]int{1, 2, 3}}},
				expr{},
				"invalid placeholder index: 2",
				4,
			},
		}

		for i, x := range examples {
			newIdx, err := x.cond.build(x.startIdx)
			if x.expectedError == "" {
				if err != nil {
					t.Fatalf("example %d: expected error to be nil, got %#v", i, err)
				}
				if x.cond.text != x.expected.text {
					t.Errorf("example %d: expected text to be %q, got %q", i, x.expected.text, x.cond.text)
				}
				if len(x.expected.params) > 0 && !reflect.DeepEqual(x.cond.params, x.expected.params) {
					t.Errorf("example %d: expected params to be %v, got %v", i, x.expected.params, x.cond.params)
				}
				if newIdx != x.expectedNewIdx {
					t.Errorf("example %d: expected newIdx to be %d, got %d", i, x.expectedNewIdx, newIdx)
				}
			} else if x.expectedError != "" && x.expectedError != err.Error() {
				t.Fatalf("example %d: expected error to be %q, got %q", i, x.expectedError, err.Error())
			}
		}
	})
}
