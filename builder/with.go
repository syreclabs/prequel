package builder

import (
	"bytes"
	"errors"
)

type with struct {
	name  string
	query Selecter
}
type withs []*with

func (b withs) build() (string, []interface{}, error) {
	if len(b) == 0 {
		return "", []interface{}{}, nil
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	buf.WriteString("WITH ")

	for i, x := range b {
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

		// validate and rename params
		c := &cond{sql, pps}
		if _, err := c.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(x.name)
		buf.WriteString(" AS (")
		buf.WriteString(c.expr)
		buf.WriteRune(')')

		if len(c.params) > 0 {
			params = append(params, c.params...)
		}
	}

	return buf.String(), params, nil
}
