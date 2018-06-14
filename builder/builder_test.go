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
	intVal := 1
	floatVal := 3.14
	structVal := struct {
		A int
		B string
	}{1, "foo"}
	arrayVal := [3]float32{1, 2, 3}
	sliceVal := []string{"a", "b"}
	mapVal := map[string]int{"foo": 1, "bar": 2}

	defaults := []interface{}{
		int(0), int8(0), int16(0), int32(0), int64(0),
		uint(0), uint8(0), uint16(0), uint32(0), uint64(0),
		float32(0), float64(0),
		[]float32{}, map[string]int{},
		(*int)(nil), (*struct{})(nil), (*[]float32)(nil), (*map[string]int)(nil),
	}
	for _, d := range defaults {
		v := Default(d)
		if _, ok := v.(DefaultValue); !ok {
			t.Errorf("expected %T(%v) to be interpreted as default", d, d)
		}
	}

	notDefaults := []interface{}{
		int(1), int8(-1), int16(1), int32(-1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1.1), float64(-1.1),
		&intVal, &floatVal,
		structVal, &structVal, arrayVal, &arrayVal,
		sliceVal, &sliceVal, mapVal, &mapVal,
		true, false,
	}
	for _, nd := range notDefaults {
		v := Default(nd)
		if _, ok := v.(DefaultValue); ok {
			t.Errorf("expected %T(%v) not to be interpreted as default", nd, nd)
		}
	}
}
