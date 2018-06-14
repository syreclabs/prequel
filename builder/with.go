package builder

import (
	"bytes"
	"errors"
)

type with struct {
	name  string
	query Builder
}

type withs []*with

func (ww withs) build() (string, []interface{}, error) {
	if len(ww) == 0 {
		return "", []interface{}{}, nil
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	buf.WriteString("WITH ")

	for i, w := range ww {
		if isBlank(w.name) {
			return "", nil, errors.New("empty query name")
		}

		if i > 0 {
			buf.WriteString(", ")
		}

		// prepare query
		sql, pps, err := w.query.Build()
		if err != nil {
			return "", nil, err
		}

		// validate and rename params
		x := &expr{sql, pps}
		if _, err := x.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString(w.name)
		buf.WriteString(" AS (")
		buf.WriteString(x.text)
		buf.WriteRune(')')

		params = append(params, x.params...)
	}

	return buf.String(), params, nil
}
