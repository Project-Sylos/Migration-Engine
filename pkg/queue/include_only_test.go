package queue

import "testing"

func TestParseIncludeOnlyJSON(t *testing.T) {
	if ParseIncludeOnlyJSON("") != nil {
		t.Fatal("empty should be unrestricted")
	}
	allow := ParseIncludeOnlyJSON(`["a","b"]`)
	if !ChildAllowedByIncludeOnly(allow, "a") || ChildAllowedByIncludeOnly(allow, "c") {
		t.Fatalf("allow=%v", allow)
	}
	if EncodeIncludeOnlyJSON([]string{"x", ""}) != `["x"]` {
		t.Fatalf("encode=%q", EncodeIncludeOnlyJSON([]string{"x", ""}))
	}
}
