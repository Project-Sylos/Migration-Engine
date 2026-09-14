package db

import (
	"path/filepath"
	"testing"
)

func TestOpenReturnsNonNil(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Path: filepath.Join(dir, "x.db"), OpsDir: filepath.Join(dir, "x.ops")})
	if database == nil && err == nil {
		t.Fatal("Open returned nil,nil")
	}
	if err != nil {
		t.Fatalf("Open err: %v", err)
	}
	if database == nil {
		t.Fatal("Open returned nil DB with nil error")
	}
	if database.Ops() == nil {
		t.Fatal("expected ops store")
	}
}
