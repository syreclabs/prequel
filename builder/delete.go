package builder

import (
	"bytes"
	"errors"
	"fmt"
)

type deleter struct {
	from  string
	where []cond
}

func (b *deleter) Where(expr string, params ...interface{}) Deleter {
	b.where = append(b.where, cond{expr, params})
	return b
}

func (b *deleter) Build() (string, []interface{}, error) {
	// verify
	if isEmpty(b.from) {
		return "", nil, errors.New("empty from")
	}

	// build
	buf := bytes.NewBufferString("delete")

	// from
	buf.WriteString(" from ")
	buf.WriteString(b.from)

	// where
	var params []interface{}

	if len(b.where) > 0 {
		buf.WriteString(" where ")
		for i, x := range b.where {
			if isEmpty(x.expr) {
				return "", nil, errors.New("empty where expression")
			}

			if !validateCondition(x) {
				return "", nil, errors.New(fmt.Sprintf("invalid where expression (%s), params (%v)", x.expr, x.params))
			}

			if i > 0 {
				buf.WriteString(" and ")
			}

			// TODO: rename params in x.expr, use len(params) as last used params

			// note: x.params may contain unused by x.expr params
			params = append(params, x.params...)

			buf.WriteString(x.expr)
		}
	}

	return buf.String(), params, nil
}
