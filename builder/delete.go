package builder

import (
	"bytes"
	"errors"
)

type deleter struct {
	with      withs
	from      string
	using     []string
	where     exprs
	returning []string
}

func (b *deleter) With(name string, q Builder) Deleter {
	b.with = append(b.with, &with{name, q})
	return b
}

func (b *deleter) Using(using string) Deleter {
	b.using = append(b.using, using)
	return b
}

func (b *deleter) Where(where string, params ...interface{}) Deleter {
	b.where = append(b.where, &expr{where, params})
	return b
}

func (b *deleter) Returning(returning ...string) Deleter {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *deleter) Build() (string, []interface{}, error) {
	// verify
	if isBlank(b.from) {
		return "", nil, errors.New("empty from")
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

	// delete
	buf.WriteString("DELETE FROM ")

	// from
	buf.WriteString(b.from)

	// using
	if len(b.using) > 0 {
		buf.WriteString(" USING ")
		for i, s := range b.using {
			if isBlank(s) {
				return "", nil, errors.New("empty using")
			}
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
