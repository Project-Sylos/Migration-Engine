// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package oauth

import (
	"bytes"
	"testing"
)

func testTokenKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func TestSealOpenOAuthCredentialsRoundTrip(t *testing.T) {
	t.Parallel()
	plain := []byte(`{"refresh_token":"secret","access_token":"tok"}`)
	key := testTokenKey(t)

	stored, err := SealOAuthCredentials(plain, key)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if stored == string(plain) {
		t.Fatal("expected encrypted storage form")
	}
	if !bytes.HasPrefix([]byte(stored), []byte(oauthCredsEncryptedPrefix)) {
		t.Fatalf("expected prefix %q, got %q", oauthCredsEncryptedPrefix, stored[:10])
	}

	got, err := OpenOAuthCredentials(stored, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %q, want %q", got, plain)
	}
}

func TestSealOAuthCredentialsPlaintextWhenNoKey(t *testing.T) {
	t.Parallel()
	plain := []byte(`{"refresh_token":"secret"}`)
	stored, err := SealOAuthCredentials(plain, nil)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if stored != string(plain) {
		t.Fatalf("got %q, want plaintext", stored)
	}
	got, err := OpenOAuthCredentials(stored, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %q, want %q", got, plain)
	}
}

func TestOpenOAuthCredentialsWrongKey(t *testing.T) {
	t.Parallel()
	plain := []byte(`{"refresh_token":"secret"}`)
	key := testTokenKey(t)
	stored, err := SealOAuthCredentials(plain, key)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	wrong := make([]byte, 32)
	_, err = OpenOAuthCredentials(stored, wrong)
	if err == nil {
		t.Fatal("expected decrypt failure with wrong key")
	}
}
