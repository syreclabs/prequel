package builder

import (
	"bytes"
	"errors"
	"fmt"
)

// TODO: suport update like
// UPDATE summary s SET (sum_x, sum_y, avg_x, avg_y) = (SELECT sum(x), sum(y), avg(x), avg(y) FROM data d WHERE d.group_id = s.group_id)

type updater struct {
	upsert    bool // tells builder if sql should be built for update or for upsert
	with      withs
	table     string
	from      []string
	set       exprs
	where     exprs
	returning []string
}

func (b *updater) With(name string, q Builder) Updater {
	b.with = append(b.with, &with{name, q})
	return b
}

func (b *updater) From(from string) Updater {
	b.from = append(b.from, from)
	return b
}

func (b *updater) Set(set string, params ...interface{}) Updater {
	b.set = append(b.set, &expr{set, params})
	return b
}

func (b *updater) SetExcluded(col ...string) Updater {
	for _, c := range col {
		b.set = append(b.set, &expr{fmt.Sprintf("%s = EXCLUDED.%s", c, c), nil})
	}
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
	if b.upsert {
		if b.with != nil && len(b.with) > 0 {
			return "", nil, errors.New("with not supported for upsert")
		}

		if len(b.from) > 0 {
			return "", nil, errors.New("from not supported for update part of upsert")
		}

		if len(b.returning) > 0 {
			return "", nil, errors.New("returning not supported for update part of upsert")
		}
	} else {
		if isBlank(b.table) {
			return "", nil, errors.New("empty table")
		}
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
	if !b.upsert {
		buf.WriteString("UPDATE ")
		buf.WriteString(b.table)
	}

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
		buf.WriteString(" FROM ")
		for i, s := range b.from {
			if i > 0 {
				buf.WriteRune(' ')
			}
			buf.WriteString(s)
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
