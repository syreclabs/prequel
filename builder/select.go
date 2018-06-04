package builder

import (
	"bytes"
	"strconv"
)

type selecter struct {
	expr     []string
	from     []string
	where    []string
	params   []interface{}
	offset   int
	limit    int
	distinct []string
}

func (b *selecter) From(from string) Selecter {
	b.from = append(b.from, from)
	return b
}

func (b *selecter) Where(cond string, params ...interface{}) Selecter {
	b.where = append(b.where, cond)
	b.params = append(b.params, params...)
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

func (b *selecter) Build() (string, []interface{}, error) {
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
			if i > 0 {
				buf.WriteString(" and ")
			}
			buf.WriteString(x)
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

	return buf.String(), b.params, nil
}
