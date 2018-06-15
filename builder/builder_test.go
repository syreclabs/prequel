package builder

import (
	"fmt"
	"testing"
)

func validateBuilderResult(generatedSql, expectedSql string, generatedParamsCound, expectedParamsCount int) error {
	if expectedSql != generatedSql {
		return fmt.Errorf("expected sql %q, got %q", expectedSql, generatedSql)
	}
	if generatedParamsCound != expectedParamsCount {
		return fmt.Errorf("expected params length to be %d, got %d", expectedParamsCount, generatedParamsCound)
	}
	return nil
}

func TestDefault(t *testing.T) {
	defaults := []interface{}{
		int(0), int8(0), int16(0), int32(0), int64(0),
		uint(0), uint8(0), uint16(0), uint32(0), uint64(0),
		float32(0), float64(0), "",
		[]int{}, map[string]int{},
		nil, (*int)(nil), (*bool)(nil), (*struct{})(nil),
		(*[]int)(nil), (*map[string]int)(nil),
	}
	for _, d := range defaults {
		v := Default(d)
		if _, ok := v.(DefaultValue); !ok {
			t.Errorf("expected %T(%v) to be interpreted as default", d, d)
		}
	}

	intVal := 1
	floatVal := 3.14
	stringVal := "string"
	structVal := struct{ a string }{"string"}
	emptyStructVal := struct{}{}
	arrayVal := [3]float32{1, 2, 3}
	sliceVal := []string{"a", "b"}
	mapVal := map[string]int{"foo": 1, "bar": 2}

	notDefaults := []interface{}{
		int(1), int8(-1), int16(2), int32(-2), int64(3),
		uint(1), uint8(2), uint16(3), uint32(4), uint64(5),
		float32(1.1), float64(-1.1),
		stringVal, &stringVal,
		structVal, &structVal, emptyStructVal, &emptyStructVal,
		&intVal, &floatVal,
		arrayVal, &arrayVal, sliceVal, &sliceVal, mapVal, &mapVal,
		true, false,
	}
	for _, nd := range notDefaults {
		v := Default(nd)
		if _, ok := v.(DefaultValue); ok {
			t.Errorf("expected %T(%v) not to be interpreted as default", nd, nd)
		}
	}
}
