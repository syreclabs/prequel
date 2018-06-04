package builder

import (
	"bytes"
	"errors"
	"fmt"
)

type inserter struct {
	into    string
	columns []string
	values  []interface{}
}

func (b *inserter) Columns(columns ...string) Inserter {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *inserter) Values(values ...interface{}) Inserter {
	b.values = append(b.values, values...)
	return b
}

func (b *inserter) Build() (string, []interface{}, error) {
	// verify
	if len(b.columns) == 0 {
		return "", nil, errors.New("empty columns")
	}

	if len(b.columns) != len(b.values) {
		return "", nil, errors.New(fmt.Sprintf("invalid number of values, expected (%d), got (%d)", len(b.columns), len(b.values)))
	}

	// build
	buf := bytes.NewBufferString("insert")

	// into
	buf.WriteString(" into ")
	buf.WriteString(b.into)

	if len(b.columns) > 0 {
		// columns
		buf.WriteString(" (")
		for i, x := range b.columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
		buf.WriteRune(')')

		// values
		buf.WriteString(" values (")
		for i, _ := range b.columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("$%d", i+1))
		}
		buf.WriteRune(')')
	}

	return buf.String(), b.values, nil
}
