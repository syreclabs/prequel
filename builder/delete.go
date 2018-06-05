package builder

import (
	"bytes"
	"errors"
)

type deleter struct {
	from  string
	where conds
}

func (b *deleter) Where(expr string, params ...interface{}) Deleter {
	b.where = append(b.where, &cond{expr, params})
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
		// validate and rename where conditions
		if err := b.where.build(len(params)); err != nil {
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

	return buf.String(), params, nil
}
