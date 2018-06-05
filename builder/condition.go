package builder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type cond struct {
	expr   string
	params []interface{}
}

type conds []*cond

func (c *cond) build(placeholderIdx int) (int, error) {
	var buf bytes.Buffer
	rr := []rune(c.expr)

	for i := 0; i < len(rr); {
		switch rr[i] {
		case '\\':
			// write this and next rune as-is
			buf.WriteRune(rr[i])
			i++
			if i < len(rr) {
				buf.WriteRune(rr[i])
				i++
			}
			continue
		case '\'':
			// find matching closing quote
			buf.WriteRune(rr[i])
			i++
			for j := i; j < len(rr); j++ {
				buf.WriteRune(rr[j])
				i++
				if rr[j] == '\\' && j+1 < len(rr) {
					buf.WriteRune(rr[j+1])
					i++
					j++
				}
				if rr[j] == '\'' {
					break
				}
			}
			// at the end of the expr
			if i == len(rr) {
				return 0, errors.New("missing closing quote")
			}
		case '$':
			buf.WriteRune(rr[i])
			i++
			var b bytes.Buffer
			for j := i; j < len(rr) && c.expr[j] >= '0' && c.expr[j] <= '9'; j++ {
				b.WriteRune(rr[j])
				i++
			}
			if b.Len() == 0 {
				return 0, errors.New("invalid placeholder")
			}
			idx, err := strconv.Atoi(b.String())
			if err != nil {
				return 0, err
			}
			if idx == 0 || idx > len(c.params) {
				return 0, fmt.Errorf("invalid placeholder index: %d", idx)
			}
			buf.WriteString(strconv.Itoa(placeholderIdx + idx))
		default:
			buf.WriteRune(rr[i])
			i++
		}
	}

	c.expr = buf.String()
	return placeholderIdx + len(c.params), nil
}

func (cc conds) build(placeholderIdx int) error {
	if placeholderIdx < 0 {
		return errors.New("negative index")
	}
	for _, c := range cc {
		if isEmpty(c.expr) {
			return errors.New("empty expression")
		}

		newIdx, err := c.build(placeholderIdx)
		if err != nil {
			return err
		}
		placeholderIdx = newIdx
	}
	return nil
}

func isEmpty(v string) bool {
	return strings.Trim(v, " ") == ""
}
