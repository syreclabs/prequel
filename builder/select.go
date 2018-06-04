package builder

import (
	"bytes"
	"errors"
	"strconv"
)

type selecter struct {
	expr     []string
	from     []string
	where    []cond
	params   []interface{}
	offset   int
	limit    int
	distinct []string
	groupby  []string
	having   []cond
}

func (b *selecter) From(from string) Selecter {
	b.from = append(b.from, from)
	return b
}

func (b *selecter) Where(expr string, params ...interface{}) Selecter {
	b.where = append(b.where, cond{expr, params})
	return b
}

func (b *selecter) Offset(offset int) Selecter {
	b.offset = offset
	return b
}

func (b *selecter) Limit(limit int) Selecter {
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
	b.having = append(b.having, cond{expr, params})
	return b
}

func (b *selecter) Build() (string, []interface{}, error) {
	var params []interface{}

	buf := bytes.NewBufferString("select")

	// distinct / distinct on
	if b.distinct != nil {
		buf.WriteString(" distinct")
	}
	if len(b.distinct) > 0 {
		buf.WriteString(" on (")
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
	buf.WriteString(" from ")
	for i, x := range b.from {
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.WriteString(x)
	}

	if len(b.where) > 0 {
		buf.WriteString(" where ")
		for i, x := range b.where {
			if isEmpty(x.expr) {
				return "", nil, errors.New("empty where expression")
			}

			// placeholderIdx, err = buildCond(x, placeholderIdx)
			// if !checkCond(x) {
			// 	return "", nil, errors.New(fmt.Sprintf("invalid where expression (%q), params (%v)", x.expr, x.params))
			// }

			if i > 0 {
				buf.WriteString(" and ")
			}

			// TODO: rename params in x.expr, use len(params) as last used params

			// note: x.params may contain unused by x.expr params
			// params = append(params, x.params...)

			buf.WriteString(x.expr)
		}
	}

	// group by
	if len(b.groupby) > 0 {
		buf.WriteString(" group by ")
		for i, x := range b.groupby {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(x)
		}
	}

	// having
	if len(b.having) > 0 {
		buf.WriteString(" having ")
		for i, x := range b.having {
			if isEmpty(x.expr) {
				return "", nil, errors.New("empty having expression")
			}

			// if !validateCondition(x) {
			// 	return "", nil, errors.New(fmt.Sprintf("invalid having expression (%s), params (%v)", x.expr, x.params))
			// }

			if i > 0 {
				buf.WriteString(" and ")
			}

			// TODO: rename params in x.expr, use len(params) as last used params

			// note: x.params may contain unused by x.expr params
			// params = append(params, x.params...)

			buf.WriteString(x.expr)
		}
	}

	// offset
	if b.offset > 0 {
		buf.WriteString(" offset ")
		buf.WriteString(strconv.Itoa(b.offset))
	}

	// limit
	if b.limit > 0 {
		buf.WriteString(" limit ")
		buf.WriteString(strconv.Itoa(b.limit))
	}

	return buf.String(), params, nil
}
