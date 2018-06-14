package builder

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

type expr struct {
	text   string
	params []interface{}
}

type exprs []*expr

func (x *expr) build(startIdx int) (int, error) {
	var buf bytes.Buffer
	var paramIdx int
	var newParams []interface{}

	rr := []rune(x.text)

	for idx := 0; idx < len(rr); {
		switch rr[idx] {
		case '\\':
			// write quoting backslash and the next rune as-is
			buf.WriteRune(rr[idx])
			idx++
			if idx < len(rr) {
				buf.WriteRune(rr[idx])
				idx++
			}
			continue
		case '\'':
			// find matching closing quote
			buf.WriteRune(rr[idx])
			idx++
			found := false
			for i := idx; i < len(rr); i++ {
				buf.WriteRune(rr[i])
				idx++
				// write quoting backslash and the next rune as-is
				if rr[i] == '\\' && i+1 < len(rr) {
					buf.WriteRune(rr[i+1])
					idx++
					i++
				}
				if rr[i] == '\'' {
					found = true
					break
				}
			}
			if !found {
				return 0, errors.New("missing closing quote")
			}
		case '$':
			idx++
			var b bytes.Buffer
			for i := idx; i < len(rr) && rr[i] >= '0' && rr[i] <= '9'; i++ {
				b.WriteRune(rr[i])
				idx++
			}
			if b.Len() == 0 {
				return 0, errors.New("invalid placeholder")
			}
			pi, err := strconv.Atoi(b.String())
			if err != nil {
				return 0, err
			}
			if pi < 1 || pi > len(x.params) {
				return 0, fmt.Errorf("invalid placeholder index: %d", pi)
			}
			pi -= 1 // placeholder index is one-based
			m := getSliceMeta(x.params[pi])
			if m != nil {
				// current placeholder is a slice, expand it
				if m.length == 0 {
					return 0, errors.New("empty slice passed as 'IN' parameter")
				}
				for i := 0; i < m.length; i++ {
					if i > 0 {
						buf.WriteRune(',')
					}
					buf.WriteRune('$')
					buf.WriteString(strconv.Itoa(startIdx + paramIdx + i))
					newParams = append(newParams, m.v.Index(i).Interface())
				}
			} else {
				// current placeholder is not a slice, just renumber it
				buf.WriteRune('$')
				buf.WriteString(strconv.Itoa(startIdx + paramIdx))
				newParams = append(newParams, x.params[pi])
			}
			paramIdx++
		default:
			buf.WriteRune(rr[idx])
			idx++
		}
	}

	x.text = buf.String()
	x.params = newParams
	return startIdx + len(x.params), nil
}

type sliceMeta struct {
	v      reflect.Value
	length int
}

func getSliceMeta(p interface{}) *sliceMeta {
	v := reflect.ValueOf(p)
	t := reflectx.Deref(v.Type())

	// []byte is a driver.Value type so it should not be expanded
	if t.Kind() == reflect.Slice && t != reflect.TypeOf([]byte{}) {
		return &sliceMeta{v, v.Len()}
	}
	return nil
}

func (xx exprs) build(startIdx int) error {
	if startIdx < 1 {
		return errors.New("start index should be >= 1")
	}
	for _, x := range xx {
		if isBlank(x.text) {
			return errors.New("empty expression")
		}
		newIdx, err := x.build(startIdx)
		if err != nil {
			return err
		}
		startIdx = newIdx
	}
	return nil
}

func isBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}
