package builder

import "testing"

func validateGeneratedSql(t *testing.T, generatedSql, expectedSql string, generatedParamsCound, expectedParamsCount int) {
	if expectedSql != generatedSql {
		t.Errorf("expected sql %q, got %q", expectedSql, generatedSql)
	}

	if generatedParamsCound != expectedParamsCount {
		t.Errorf("expected params length to be %d, got %d", expectedParamsCount, generatedParamsCound)
	}
}
