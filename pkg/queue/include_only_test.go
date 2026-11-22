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

func TestIncludeOnlyJSON_preservesTrailingSpaceServiceID(t *testing.T) {
	id := "/home/loganm/testing-gpl/Extra Space "
	enc := EncodeIncludeOnlyJSON([]string{id})
	allow := ParseIncludeOnlyJSON(enc)
	if !ChildAllowedByIncludeOnly(allow, id) {
		t.Fatalf("allowlist lost trailing space: enc=%s allow=%v", enc, allow)
	}
	if ChildAllowedByIncludeOnly(allow, "/home/loganm/testing-gpl/Extra Space") {
		t.Fatal("trimmed path must not match trailing-space ServiceID")
	}
}
