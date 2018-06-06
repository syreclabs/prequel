package builder

import (
	"bytes"
	"errors"
)

// TODO: suport update like
// UPDATE summary s SET (sum_x, sum_y, avg_x, avg_y) = (SELECT sum(x), sum(y), avg(x), avg(y) FROM data d WHERE d.group_id = s.group_id)

type updater struct {
	with      statements
	table     string
	from      []string
	set       conds
	where     conds
	returning []string
}

func (b *updater) With(name string, query Selecter) Updater {
	b.with = append(b.with, &statement{name, query})
	return b
}

func (b *updater) From(from string) Updater {
	b.from = append(b.from, from)
	return b
}

func (b *updater) Set(expr string, params ...interface{}) Updater {
	b.set = append(b.set, &cond{expr, params})
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

	if len(b.set) == 0 {
		return "", nil, errors.New("empty set")
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	// with
	if b.with != nil && len(b.with) > 0 {
		buf.WriteString("WITH")

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

			buf.WriteRune(' ')
			buf.WriteString(x.name)
			buf.WriteString(" AS (")
			buf.WriteString(sql)
			buf.WriteRune(')')

			if len(pps) > 0 {
				params = append(params, pps...)
			}
		}

		buf.WriteString(" ")
	}

	// update
	buf.WriteString("UPDATE ")
	buf.WriteString(b.table)

	// validate and rename set conditions
	if err := b.set.build(len(params) + 1); err != nil {
		return "", nil, err
	}
	buf.WriteString(" SET ")
	for i, x := range b.set {
		if i > 0 {
			buf.WriteString(", ")
		}

		params = append(params, x.params...)
		buf.WriteString(x.expr)
	}

	// from
	if len(b.from) > 0 {
		buf.WriteString(" FROM ")
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
		if err := b.where.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(" WHERE ")
		for i, x := range b.where {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			params = append(params, x.params...)
			buf.WriteString(x.expr)
		}
	}

	// returning
	if len(b.returning) > 0 {
		buf.WriteString(" RETURNING ")
		for i, x := range b.returning {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
	}

	return buf.String(), params, nil

}
