package builder

import "strings"

func isEmpty(v string) bool {
	return strings.Trim(v, " ") == ""
}

func validateCondition(x cond) bool {
	// TODO: check that x.expr contain only paramns $ that present in x.params
	return true
}
