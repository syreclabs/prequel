package builder

import (
	"bytes"
	"errors"
	"fmt"
)

type statement struct {
	name  string
	query Selecter
}
type statements []*statement

type deleter struct {
	with      statements
	from      string
	using     []string
	where     conds
	returning []string
}

func (b *deleter) With(name string, query Selecter) Deleter {
	b.with = append(b.with, &statement{name, query})
	return b
}

func (b *deleter) Using(using string) Deleter {
	b.using = append(b.using, using)
	return b
}

func (b *deleter) Where(expr string, params ...interface{}) Deleter {
	b.where = append(b.where, &cond{expr, params})
	return b
}

func (b *deleter) Returning(returning ...string) Deleter {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *deleter) Build() (string, []interface{}, error) {
	// verify
	if isEmpty(b.from) {
		return "", nil, errors.New("empty from")
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

	// delete
	buf.WriteString("delete")

	// from
	buf.WriteString(" from ")
	buf.WriteString(b.from)

	// using
	if len(b.using) > 0 {
		buf.WriteString(" using ")
		for i, x := range b.using {
			if isEmpty(x) {
				return "", nil, errors.New("empty using")
			}

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
