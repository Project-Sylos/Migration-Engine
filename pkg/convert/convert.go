// Package convert provides generic coercion of dynamically-typed values
// (for example map[string]any decoded from engine metrics) into concrete Go types.
package convert

// Number is the set of numeric types ToNumber can produce.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// ToNumber coerces an arbitrary value into the requested numeric type T. It handles
// the common numeric dynamic types and returns the zero value for T when v is nil or
// not a recognized number.
func ToNumber[T Number](v any) T {
	switch t := v.(type) {
	case int:
		return T(t)
	case int8:
		return T(t)
	case int16:
		return T(t)
	case int32:
		return T(t)
	case int64:
		return T(t)
	case uint:
		return T(t)
	case uint8:
		return T(t)
	case uint16:
		return T(t)
	case uint32:
		return T(t)
	case uint64:
		return T(t)
	case float32:
		return T(t)
	case float64:
		return T(t)
	default:
		var zero T
		return zero
	}
}

// ToBool coerces an arbitrary value into a bool, returning false for non-bool values.
func ToBool(v any) bool {
	b, _ := v.(bool)
	return b
}

// ToString coerces an arbitrary value into a string, returning "" for non-string values.
func ToString(v any) string {
	s, _ := v.(string)
	return s
}
