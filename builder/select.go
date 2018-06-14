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
	columns  exprs
	from     []string
	where    exprs
	groupBy  []string
	having   exprs
	union    unions
	orderBy  []string
	offset   uint64
	limit    uint64
	locking  string
}

func (b *selecter) With(name string, q Builder) Selecter {
	b.with = append(b.with, &with{name, q})
	return b
}

func (b *selecter) Columns(col string, params ...interface{}) Selecter {
	b.columns = append(b.columns, &expr{col, params})
	return b
}

func (b *selecter) From(from string) Selecter {
	b.from = append(b.from, from)
	return b
}

func (b *selecter) Where(where string, params ...interface{}) Selecter {
	b.where = append(b.where, &expr{where, params})
	return b
}

func (b *selecter) Distinct(distinct ...string) Selecter {
	if b.distinct == nil {
		b.distinct = []string{}
	}
	b.distinct = append(b.distinct, distinct...)
	return b
}

func (b *selecter) GroupBy(groupBy string) Selecter {
	b.groupBy = append(b.groupBy, groupBy)
	return b
}

func (b *selecter) Having(having string, params ...interface{}) Selecter {
	b.having = append(b.having, &expr{having, params})
	return b
}

func (b *selecter) Union(all bool, q Selecter) Selecter {
	b.union = append(b.union, &union{all, q})
	return b
}

func (b *selecter) OrderBy(orderBy string) Selecter {
	b.orderBy = append(b.orderBy, orderBy)
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
		params = append(params, pps...)
	}

	// select
	buf.WriteString("SELECT")

	// distinct / distinct on
	if b.distinct != nil {
		buf.WriteString(" DISTINCT")
	}
	if len(b.distinct) > 0 {
		buf.WriteString(" ON (")
		for i, s := range b.distinct {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
		}
		buf.WriteRune(')')
	}

	// select columns
	if len(b.columns) > 0 {
		buf.WriteRune(' ')
		// validate and rename SELECT expressions
		if err := b.columns.build(len(params) + 1); err != nil {
			return "", nil, err
		}
		for i, x := range b.columns {
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

	// group by
	if len(b.groupBy) > 0 {
		buf.WriteString(" GROUP BY ")
		for i, s := range b.groupBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
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
			buf.WriteString(x.text)
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
		x := &expr{sql, pps}
		if _, err := x.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(x.text)
		params = append(params, x.params...)
	}

	// order by
	if len(b.orderBy) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, s := range b.orderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s)
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
