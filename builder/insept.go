package builder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type insecter struct {
	table     string
	columns   []string
	values    []interface{}
	where     exprs
	returning []string
}

func (b *insecter) Columns(col ...string) Insecter {
	b.columns = append(b.columns, col...)
	return b
}

func (b *insecter) Values(values ...interface{}) Insecter {
	b.values = append(b.values, values...)
	return b
}

func (b *insecter) Where(where string, params ...interface{}) Insecter {
	b.where = append(b.where, &expr{where, params})
	return b
}

func (b *insecter) Returning(returning ...string) Insecter {
	b.returning = append(b.returning, returning...)
	return b
}

func (b *insecter) buildWith() withs {
	res := withs{}

	// select
	bSel := Select(b.returning...).From(b.table)
	if len(b.where) > 0 {
		for _, x := range b.where {
			bSel.Where(x.text, x.params...)
		}
	}
	res = append(res, &with{"sel", bSel})

	// insert
	var buf bytes.Buffer
	var vals []interface{}
	for i, v := range b.values {
		if i > 0 {
			buf.WriteString(", ")
		}
		if _, ok := v.(DefaultValue); ok {
			buf.WriteString("DEFAULT")
		} else {
			vals = append(vals, v)
			buf.WriteRune('$')
			buf.WriteString(strconv.Itoa(len(vals)))
		}
	}

	bIns := Insert(b.table).
		Columns(b.columns...).
		From(Select().
			Columns(buf.String(), vals...).
			Where("NOT EXISTS(SELECT * FROM sel)")).
		Returning(b.returning...)

	res = append(res, &with{"ins", bIns})
	return res
}

func (b *insecter) Build() (string, []interface{}, error) {
	// verify
	if isBlank(b.table) {
		return "", nil, errors.New("empty table")
	}

	if len(b.columns) == 0 {
		return "", nil, errors.New("empty columns")
	}

	if len(b.values) == 0 {
		return "", nil, errors.New("empty values")
	}

	if len(b.columns) != len(b.values) {
		return "", nil, fmt.Errorf("invalid number of values, expected %d, got %d", len(b.columns), len(b.values))
	}

	// Returning: ALL if nothing specified
	if len(b.returning) == 0 {
		b.Returning("*")
	}

	// build
	var params []interface{}
	var buf bytes.Buffer

	// with
	if with := b.buildWith(); with != nil && len(with) > 0 {
		sql, pps, err := with.build()
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(sql)
		buf.WriteRune(' ')
		params = append(params, pps...)
	}

	// insect
	sql, _, err := Select(b.returning...).From("ins").Union(true, Select(b.returning...).From("sel")).Build()
	if err != nil {
		return "", nil, err
	}
	buf.WriteString(sql)

	return buf.String(), params, nil
}
