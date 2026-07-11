// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

// Package sftptest provides shared helpers for optional SFTP integration scenario runners.
package sftptest

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	_ "codeberg.org/Sylos/Sylos-FS/pkg/fs/sftp"
	sftpfs "codeberg.org/Sylos/Sylos-FS/pkg/fs/sftp"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// CredentialsFromEnv loads SFTP connection settings from SYLOS_SFTP_TEST_* env vars.
func CredentialsFromEnv() (cloud.StoredCredentials, error) {
	host := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_HOST"))
	if host == "" {
		return cloud.StoredCredentials{}, fmt.Errorf("set SYLOS_SFTP_TEST_HOST to run SFTP integration tests")
	}
	port := 22
	if raw := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_PORT")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return cloud.StoredCredentials{}, fmt.Errorf("invalid SYLOS_SFTP_TEST_PORT: %w", err)
		}
		port = parsed
	}
	user := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_USER"))
	password := os.Getenv("SYLOS_SFTP_TEST_PASSWORD")
	privateKey := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_KEY"))
	if privateKey == "" {
		if keyPath := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_KEY_FILE")); keyPath != "" {
			raw, err := os.ReadFile(keyPath)
			if err != nil {
				return cloud.StoredCredentials{}, fmt.Errorf("read SYLOS_SFTP_TEST_KEY_FILE: %w", err)
			}
			privateKey = string(raw)
		}
	}
	hostKey := strings.TrimSpace(os.Getenv("SYLOS_SFTP_TEST_HOST_KEY"))
	if hostKey == "" {
		probe, err := sftpfs.FetchServerHostKey(host, port)
		if err != nil {
			return cloud.StoredCredentials{}, fmt.Errorf(
				"set SYLOS_SFTP_TEST_HOST_KEY or ensure host is reachable for probe: %w",
				err,
			)
		}
		hostKey = probe.HostKey
	}
	stored := cloud.StoredCredentialsFromSFTP(
		host,
		user,
		password,
		privateKey,
		os.Getenv("SYLOS_SFTP_TEST_KEY_PASSPHRASE"),
		hostKey,
		port,
	)
	if err := stored.ValidateSFTP(); err != nil {
		return cloud.StoredCredentials{}, err
	}
	return stored, nil
}

func RootFolder(absPath string) types.Folder {
	absPath = strings.ReplaceAll(strings.TrimSpace(absPath), "\\", "/")
	return types.Folder{
		ServiceID:    absPath,
		ParentId:     "",
		ParentPath:   "",
		DisplayName:  filepath.Base(absPath),
		LocationPath: "/",
		LastUpdated:  time.Now().UTC().Format(time.RFC3339),
		DepthLevel:   0,
		Type:         types.NodeTypeFolder,
	}
}

// NewAdapter dials SFTP and returns an adapter rooted at rootPath.
func NewAdapter(stored cloud.StoredCredentials, rootPath, connectionID string) (types.FSAdapter, error) {
	factory, err := cloud.Factory(cloud.ProviderSFTP)
	if err != nil {
		return nil, err
	}
	session, err := factory.NewSession(connectionID, stored, cloud.DefaultTokenStore, types.NewFSDegradationState())
	if err != nil {
		return nil, err
	}
	return session.CreateAdapter(RootFolder(rootPath))
}
