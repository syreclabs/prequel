package builder

import (
	"bytes"
	"errors"
	"fmt"
)

// TODO: suport update like
// UPDATE summary s SET (sum_x, sum_y, avg_x, avg_y) = (SELECT sum(x), sum(y), avg(x), avg(y) FROM data d WHERE d.group_id = s.group_id)

type updater struct {
	table     string
	from      []string
	columns   []string
	values    []interface{}
	where     conds
	returning []string
}

func (b *updater) From(from string) Updater {
	b.from = append(b.from, from)
	return b
}

func (b *updater) Columns(columns ...string) Updater {
	b.columns = append(b.columns, columns...)
	return b
}

func (b *updater) Values(values ...interface{}) Updater {
	b.values = append(b.values, values...)
	return b
}

func (b *updater) Where(expr string, params ...interface{}) Updater {
	b.where = append(b.where, &cond{expr, params})
	return b
}

func (b *updater) Returning(returning ...string) Updater {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *updater) Build() (string, []interface{}, error) {
	// verify
	if isEmpty(b.table) {
		return "", nil, errors.New("empty table")
	}

	if len(b.columns) == 0 {
		return "", nil, errors.New("empty columns")
	}

	if len(b.columns) != len(b.values) {
		return "", nil, errors.New(fmt.Sprintf("invalid number of values, expected (%d), got (%d)", len(b.columns), len(b.values)))
	}

	// build
	var params []interface{}
	buf := bytes.NewBufferString("update ")
	buf.WriteString(b.table)

	// columns
	buf.WriteString(" set ")
	for i, x := range b.columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s = $%d", x, i+1))
	}
	params = append(params, b.values...)

	// from
	if len(b.from) > 0 {
		buf.WriteString(" from ")
		for i, x := range b.from {
			if i > 0 {
				buf.WriteRune(' ')
			}
			buf.WriteString(x)
		}
	}

	// where
	if len(b.where) > 0 {
		// validate and rename where conditions
		if err := b.where.build(len(params)); err != nil {
			return "", nil, err
		}

		buf.WriteString(" where ")
		for i, x := range b.where {
			if i > 0 {
				buf.WriteString(" and ")
			}
			params = append(params, x.params...)
			buf.WriteString(x.expr)
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
