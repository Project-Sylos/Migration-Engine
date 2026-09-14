package convert

import (
	"encoding/json"
	"testing"
)

func TestToNumberFloat64FromJSONKinds(t *testing.T) {
	if got := ToNumber[float64](1.5); got != 1.5 {
		t.Fatalf("float64: %v", got)
	}
	if got := ToNumber[float64](json.Number("123.25")); got != 123.25 {
		t.Fatalf("json.Number: %v", got)
	}
	if got := ToNumber[float64]("4096"); got != 4096 {
		t.Fatalf("string: %v", got)
	}
	if got := ToNumber[int64](float64(42)); got != 42 {
		t.Fatalf("float64 to int64: %v", got)
	}
	if got := ToNumber[float64](nil); got != 0 {
		t.Fatalf("nil: %v", got)
	}
}
