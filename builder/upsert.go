package builder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type upserter struct {
	with             withs
	into             string
	columns          []string
	values           [][]interface{}
	from             Selecter
	onConflictTarget *expr
	onConflictUpdate *expr
	returning        []string
}

func (b *upserter) With(name string, q Builder) Upserter {
	b.with = append(b.with, &with{name, q})
	return b
}

func (b *upserter) Columns(col ...string) Upserter {
	b.columns = append(b.columns, col...)
	return b
}

func (b *upserter) Values(values ...interface{}) Upserter {
	b.values = append(b.values, values)
	return b
}

func (b *upserter) From(q Selecter) Upserter {
	b.from = q
	return b
}

func (b *upserter) Update(update string, params ...interface{}) Upserter {
	b.onConflictUpdate = &expr{update, params}
	return b
}

func (b *upserter) Returning(returning ...string) Upserter {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *upserter) Build() (string, []interface{}, error) {
	// verify
	if len(b.columns) > 0 && len(b.values) > 0 {
		for _, row := range b.values {
			if len(b.columns) != len(row) {
				return "", nil, fmt.Errorf("invalid number of values, expected %d, got %d", len(b.columns), len(row))
			}
		}
	}

	if b.from != nil && len(b.values) > 0 {
		return "", nil, errors.New("values must be empty if from is specified")
	}

	if b.onConflictTarget != nil {
		if isBlank(b.onConflictTarget.text) {
			return "", nil, errors.New("empty ON CONFLICT target")
		}
	}

	if b.onConflictUpdate != nil {
		if b.onConflictTarget == nil {
			return "", nil, errors.New("empty ON CONFLICT target")
		}

		if isBlank(b.onConflictUpdate.text) {
			return "", nil, errors.New("empty ON CONFLICT update statement")
		}
	}

	if b.onConflictTarget != nil && b.onConflictUpdate == nil {
		if len(b.columns) == 0 {
			return "", nil, errors.New("columns required for empty ON CONFLICT update statement")
		}
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	// with
	if b.with != nil && len(b.with) > 0 {
		sql, pps, err := b.with.build()
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(sql)
		buf.WriteRune(' ')
		params = append(params, pps...)
	}

	// insert
	buf.WriteString("INSERT")

	// into
	buf.WriteString(" INTO ")
	buf.WriteString(b.into)

	// columns
	if len(b.columns) > 0 {
		buf.WriteString(" (")
		for i, s := range b.columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
		}
		buf.WriteRune(')')
	}

	// values
	if len(b.values) > 0 {
		buf.WriteString(" VALUES ")
		for j, row := range b.values {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("(")
			for i, v := range row {
				if i > 0 {
					buf.WriteString(", ")
				}
				if _, ok := v.(DefaultValue); ok {
					buf.WriteString("DEFAULT")
				} else {
					params = append(params, v)
					buf.WriteRune('$')
					buf.WriteString(strconv.Itoa(len(params)))
				}
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

		// validate and rename params
		x := &expr{sql, pps}
		if _, err := x.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(x.text)
		params = append(params, x.params...)
	}

	// on conflict: do nothing
	if b.onConflictTarget != nil {
		buf.WriteString(" ON CONFLICT ")

		// validate and rename target condition
		if _, err := b.onConflictTarget.build(len(params) + 1); err != nil {
			return "", nil, err
		}
		buf.WriteString(b.onConflictTarget.text)
		params = append(params, b.onConflictTarget.params...)

		buf.WriteString(" DO UPDATE SET ")

		// use update statement if provided
		if b.onConflictUpdate != nil {
			// validate and rename target condition
			if _, err := b.onConflictUpdate.build(len(params) + 1); err != nil {
				return "", nil, err
			}

			buf.WriteString(b.onConflictUpdate.text)
			params = append(params, b.onConflictUpdate.params...)
		} else {
			// otherwise generate EXCLUDED for columns
			for i, s := range b.columns {
				if i > 0 {
					buf.WriteString(", ")
				}

				buf.WriteString(fmt.Sprintf("%s = EXCLUDED.%s", s, s))
			}
		}
	}

	// returning
	if len(b.returning) > 0 {
		buf.WriteString(" RETURNING ")
		for i, s := range b.returning {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
		}
	}
	return buf.String(), params, nil
}
