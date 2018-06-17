package builder

import (
	"bytes"
	"errors"
)

// TODO: suport update like
// UPDATE summary s SET (sum_x, sum_y, avg_x, avg_y) = (SELECT sum(x), sum(y), avg(x), avg(y) FROM data d WHERE d.group_id = s.group_id)

type updater struct {
	with      withs
	table     string
	from      exprs
	set       exprs
	where     exprs
	returning []string
}

func (b *updater) With(name string, q Builder) Updater {
	b.with = append(b.with, &with{name, q})
	return b
}

func (b *updater) From(from string, params ...interface{}) Updater {
	b.from = append(b.from, &expr{from, params})
	return b
}

func (b *updater) Set(set string, params ...interface{}) Updater {
	b.set = append(b.set, &expr{set, params})
	return b
}

func (b *updater) Where(where string, params ...interface{}) Updater {
	b.where = append(b.where, &expr{where, params})
	return b
}

func (b *updater) Returning(returning ...string) Updater {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *updater) Build() (string, []interface{}, error) {
	// verify
	if isBlank(b.table) {
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
		sql, pps, err := b.with.build()
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(sql)
		buf.WriteRune(' ')
		params = append(params, pps...)
	}

	// update
	buf.WriteString("UPDATE ")
	buf.WriteString(b.table)

	// set
	if len(b.set) > 0 {
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
			buf.WriteString(x.text)
		}
	}

	// from
	if len(b.from) > 0 {
		// validate and rename from conditions
		if err := b.from.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(" FROM ")
		for i, x := range b.from {
			if i > 0 {
				buf.WriteRune(' ')
			}
			params = append(params, x.params...)
			buf.WriteString(x.text)
		}
	}

	// where
	if len(b.where) > 0 {
		// validate and rename where conditions
		if err := b.where.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(" WHERE (")
		for i, x := range b.where {
			if i > 0 {
				buf.WriteString(") AND (")
			}
			params = append(params, x.params...)
			buf.WriteString(x.text)
		}
		buf.WriteRune(')')
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
