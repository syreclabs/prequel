package builder

import (
	"bytes"
	"strconv"
)

type union struct {
	all   bool
	query Selecter
}

type unions []*union

type selecter struct {
	with     withs
	distinct []string
	expr     conds
	from     []string
	where    conds
	groupby  []string
	having   conds
	union    unions
	orderby  []string
	offset   uint64
	limit    uint64
	locking  string
}

func (b *selecter) With(name string, query Builder) Selecter {
	b.with = append(b.with, &with{name, query})
	return b
}

func (b *selecter) Columns(expr string, params ...interface{}) Selecter {
	b.expr = append(b.expr, &cond{expr, params})
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

func (b *selecter) Union(all bool, query Selecter) Selecter {
	b.union = append(b.union, &union{all, query})
	return b
}

func (b *selecter) OrderBy(expr string) Selecter {
	b.orderby = append(b.orderby, expr)
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

func (b *selecter) For(locking string) Selecter {
	b.locking = locking
	return b
}

func (b *selecter) Build() (string, []interface{}, error) {
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

		if len(pps) > 0 {
			params = append(params, pps...)
		}
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
	if len(b.expr) > 0 {
		buf.WriteRune(' ')
		// validate and rename where conditions
		if err := b.expr.build(len(params) + 1); err != nil {
			return "", nil, err
		}
		for i, x := range b.expr {
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

	// union
	for _, union := range b.union {
		buf.WriteString(" UNION ")
		if union.all {
			buf.WriteString("ALL ")
		}

		// prepare query
		sql, pps, err := union.query.Build()
		if err != nil {
			return "", nil, err
		}

		// validate and rename params
		c := &cond{sql, pps}
		if _, err := c.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(c.expr)
		if len(c.params) > 0 {
			params = append(params, c.params...)
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

	// for
	if b.locking != "" {
		buf.WriteString(" FOR ")
		buf.WriteString(b.locking)
	}

	return buf.String(), params, nil
}
