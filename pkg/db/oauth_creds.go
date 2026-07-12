// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/base64"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

const oauthCredsEncryptedPrefix = "enc:v1:"

// SealOAuthCredentials stores OAuth credential JSON. When tokenKey is nil, plaintext JSON is returned for tests.
// Production API opens pass the per-migration 32-byte key from sylos.duckdb.
func SealOAuthCredentials(plaintextJSON []byte, tokenKey []byte) (string, error) {
	if len(tokenKey) == 0 {
		return string(plaintextJSON), nil
	}
	if len(tokenKey) != credentials.KeySize {
		return "", credentials.ErrInvalidKeyLength
	}
	sealed, err := credentials.Encrypt(plaintextJSON, tokenKey)
	if err != nil {
		return "", fmt.Errorf("seal oauth credentials: %w", err)
	}
	return oauthCredsEncryptedPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// OpenOAuthCredentials returns OAuth credential JSON from a stored row value.
// Plaintext legacy rows (no enc:v1: prefix) are returned as-is when tokenKey is nil or for backward compatibility.
func OpenOAuthCredentials(stored string, tokenKey []byte) ([]byte, error) {
	if !strings.HasPrefix(stored, oauthCredsEncryptedPrefix) {
		return []byte(stored), nil
	}
	if len(tokenKey) == 0 {
		return nil, fmt.Errorf("oauth credentials are encrypted but no token key was provided")
	}
	if len(tokenKey) != credentials.KeySize {
		return nil, credentials.ErrInvalidKeyLength
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, oauthCredsEncryptedPrefix))
	if err != nil {
		return nil, fmt.Errorf("decode oauth credentials: %w", err)
	}
	plain, err := credentials.Decrypt(raw, tokenKey)
	if err != nil {
		return nil, fmt.Errorf("open oauth credentials: %w", err)
	}
	return plain, nil
}
