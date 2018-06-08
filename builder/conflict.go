package builder

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	onConflictNothing = "NOTHING"
	onConflictUpdate  = "UPDATE"
)

type conflicter struct {
	action string
	target string
	where  conds
	update Updater
}

func (b *conflicter) Target(target string) Conflicter {
	b.target = target
	return b
}

func (b *conflicter) Where(expr string, params ...interface{}) Conflicter {
	b.where = append(b.where, &cond{expr, params})
	return b
}

func (b *conflicter) Update(query Updater) Conflicter {
	b.update = query
	return b
}

func (b *conflicter) Build() (string, []interface{}, error) {
	// validation
	switch b.action {
	case onConflictNothing:
		// it is optional to specify target, no need to check for emptiness
		if b.update != nil {
			return "", nil, fmt.Errorf("update not supported for action %s", onConflictNothing)
		}
	case onConflictUpdate:
		if isEmpty(b.target) {
			return "", nil, errors.New("empty target")
		}

		if b.update == nil {
			return "", nil, fmt.Errorf("update required for action %s", onConflictUpdate)
		}
	default:
		return "", nil, fmt.Errorf("unsupported action %s", b.action)
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	buf.WriteString("ON CONFLICT ")

	// target is optional
	if len(b.target) > 0 {
		buf.WriteString(b.target)
		buf.WriteString(" ")
	}

	// where is optional
	if len(b.where) > 0 {
		if err := b.where.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString("WHERE ")
		for i, x := range b.where {
			if i > 0 {
				buf.WriteString(" AND ")
			}
			params = append(params, x.params...)
			buf.WriteString(x.expr)
		}
		buf.WriteRune(' ')
	}

	// action required
	switch b.action {
	case onConflictNothing:
		buf.WriteString("DO NOTHING")
	case onConflictUpdate:
		// prepare query
		sql, pps, err := b.update.Build()
		if err != nil {
			return "", nil, err
		}

		// validate and rename
		c := &cond{sql, pps}
		if _, err := c.build(len(params) + 1); err != nil {
			return "", nil, err
		}

		buf.WriteString("DO UPDATE")
		buf.WriteString(c.expr)

		if len(c.params) > 0 {
			params = append(params, c.params...)
		}
	}

	return buf.String(), params, nil
}
