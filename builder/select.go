package builder

import (
	"bytes"
	"errors"
	"strconv"
)

type selecter struct {
	with     statements
	expr     []string
	from     []string
	where    conds
	params   []interface{}
	offset   uint64
	limit    uint64
	distinct []string
	groupby  []string
	orderby  []string
	having   conds
}

func (b *selecter) With(name string, query Selecter) Selecter {
	b.with = append(b.with, &statement{name, query})
	return b
}

func (b *selecter) From(from string) Selecter {
	b.from = append(b.from, from)
	return b
}

func (b *selecter) Where(expr string, params ...interface{}) Selecter {
	b.where = append(b.where, &cond{expr, params})
	return b
}

func (b *selecter) Offset(offset uint64) Selecter {
	b.offset = offset
	return b
}

func (b *selecter) Limit(limit uint64) Selecter {
	b.limit = limit
	return b
}

func (b *selecter) Distinct(expr ...string) Selecter {
	if b.distinct == nil {
		b.distinct = []string{}
	}
	b.distinct = append(b.distinct, expr...)
	return b
}

func (b *selecter) GroupBy(expr string) Selecter {
	b.groupby = append(b.groupby, expr)
	return b
}

func (b *selecter) Having(expr string, params ...interface{}) Selecter {
	b.having = append(b.having, &cond{expr, params})
	return b
}

func (b *selecter) OrderBy(expr string) Selecter {
	b.orderby = append(b.orderby, expr)
	return b
}

func (b *selecter) ForUpdate() Selecter {
	// TODO
	return b
}

func (b *selecter) Build() (string, []interface{}, error) {
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

		buf.WriteRune(' ')
	}

	// select
	buf.WriteString("SELECT")

	// distinct / distinct on
	if b.distinct != nil {
		buf.WriteString(" DISTINCT")
	}
	if len(b.distinct) > 0 {
		buf.WriteString(" ON (")
		for i, x := range b.distinct {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
		buf.WriteRune(')')
	}

	// select expr
	buf.WriteRune(' ')
	for i, x := range b.expr {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(x)
	}

	// from
	buf.WriteString(" FROM ")
	for i, x := range b.from {
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.WriteString(x)
	}

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

	// group by
	if len(b.groupby) > 0 {
		buf.WriteString(" GROUP BY ")
		for i, x := range b.groupby {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
	}

	// having
	if len(b.having) > 0 {
		// validate and rename where conditions
		if err := b.having.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(" HAVING ")
		for i, x := range b.having {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			params = append(params, x.params...)
			buf.WriteString(x.expr)
		}
	}

	// order by
	if len(b.orderby) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, x := range b.orderby {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
	}

	// offset
	if b.offset > 0 {
		buf.WriteString(" OFFSET ")
		buf.WriteString(strconv.FormatUint(b.offset, 10))
	}

	// limit
	if b.limit > 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.FormatUint(b.limit, 10))
	}

	return buf.String(), params, nil
}
