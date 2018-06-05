package builder

import (
	"bytes"
	"errors"
	"fmt"
)

// TODO: on conflict
type inserter struct {
	with      statements
	into      string
	columns   []string
	values    [][]interface{}
	from      Selecter
	returning []string
}

func (b *inserter) With(name string, query Selecter) Inserter {
	b.with = append(b.with, &statement{name, query})
	return b
}

func (b *inserter) Columns(columns ...string) Inserter {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *inserter) Values(values ...interface{}) Inserter {
	b.values = append(b.values, values)
	return b
}

func (b *inserter) From(query Selecter) Inserter {
	b.from = query
	return b
}

func (b *inserter) Returning(returning ...string) Inserter {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *inserter) Build() (string, []interface{}, error) {
	// verify
	if len(b.columns) > 0 && len(b.values) > 0 {
		for _, row := range b.values {
			if len(b.columns) != len(row) {
				return "", nil, errors.New(fmt.Sprintf("invalid number of values, expected (%d), got (%d)", len(b.columns), len(row)))
			}
		}
	}

	if b.from != nil && len(b.values) > 0 {
		return "", nil, errors.New("values must be empty if from specified")
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	// with
	if b.with != nil && len(b.with) > 0 {
		buf.WriteString("with")

		for i, x := range b.with {
			if isEmpty(x.name) {
				return "", nil, errors.New("empty query name")
			}

			if i > 0 {
				buf.WriteString(", ")
			}

			// prepare query
			sql, pps, err := x.query.Build()
			if err != nil {
				return "", nil, err
			}

			buf.WriteString(fmt.Sprintf(" %s as (%s)", x.name, sql))

			if len(pps) > 0 {
				params = append(params, pps...)
			}
		}

		buf.WriteString(" ")
	}

	// insert
	buf.WriteString("insert")

	// into
	buf.WriteString(" into ")
	buf.WriteString(b.into)

	// columns
	if len(b.columns) > 0 {
		buf.WriteString(" (")
		for i, x := range b.columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
		buf.WriteRune(')')
	}

	// values
	if len(b.values) > 0 {
		buf.WriteString(" values ")
		for j, row := range b.values {
			if j > 0 {
				buf.WriteString(", ")
			}

			params = append(params, row...)
			buf.WriteString("(")
			for i, _ := range row {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fmt.Sprintf("$%d", j*len(row)+i+1))
			}
			buf.WriteRune(')')
		}
	}

	if b.from != nil {
		buf.WriteRune(' ')
		// prepare query
		sql, pps, err := b.from.Build()
		if err != nil {
			return "", nil, err
		}

		buf.WriteString(sql)

		if len(pps) > 0 {
			params = append(params, pps...)
		}
	}

	// returning
	if len(b.returning) > 0 {
		buf.WriteString(" returning ")
		for i, x := range b.returning {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
	}

	return buf.String(), params, nil
}
