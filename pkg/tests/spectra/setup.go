// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
)

// setupTest assembles the Spectra-backed migration configuration.
func setupTest() (migration.Config, error) {
	fmt.Println("Loading Spectra configuration...")

	// Clean up any previous Spectra state
	if _, err := os.Stat("./spectra.db"); err == nil {
		os.Remove("./spectra.db")
	}

	spectraFS, err := sdk.New("pkg/configs/spectra.json")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to initialize Spectra: %w", err)
	}

	srcRoot, dstRoot, err := loadSpectraRoots(spectraFS)
	if err != nil {
		return migration.Config{}, err
	}

	srcAdapter, err := fsservices.NewSpectraFS(spectraFS, srcRoot.Id, "primary")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create src adapter: %w", err)
	}

	dstAdapter, err := fsservices.NewSpectraFS(spectraFS, dstRoot.Id, "s1")
	if err != nil {
		return migration.Config{}, fmt.Errorf("failed to create dst adapter: %w", err)
	}

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           "pkg/tests/migration_test.db",
			RemoveExisting: true,
		},
		Source: migration.Service{
			Name:    "Spectra-Primary",
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    "Spectra-S1",
			Adapter: dstAdapter,
		},
		SeedRoots:       true,
		WorkerCount:     10,
		MaxRetries:      3,
		CoordinatorLead: 4,
		LogAddress:      "127.0.0.1:8081",
		LogLevel:        "trace",
		StartupDelay:    3 * time.Second,
		Verification:    migration.VerifyOptions{},
	}

	if err := cfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return cfg, nil
}

// loadSpectraRoots fetches the Spectra root nodes and maps them to fsservices.Folder structures.
func loadSpectraRoots(spectraFS *sdk.SpectraFS) (fsservices.Folder, fsservices.Folder, error) {
	// Get root nodes from Spectra using request structs
	srcRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fsservices.Folder{}, fsservices.Folder{}, fmt.Errorf("failed to get src root from Spectra: %w", err)
	}

	dstRoot, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: "root",
	})
	if err != nil {
		return fsservices.Folder{}, fsservices.Folder{}, fmt.Errorf("failed to get dst root from Spectra: %w", err)
	}

	// Create folder structs
	srcFolder := fsservices.Folder{
		Id:           srcRoot.ID,
		ParentId:     "",
		DisplayName:  srcRoot.Name,
		LocationPath: "/",
		LastUpdated:  srcRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         fsservices.NodeTypeFolder,
	}

	dstFolder := fsservices.Folder{
		Id:           dstRoot.ID,
		ParentId:     "",
		DisplayName:  dstRoot.Name,
		LocationPath: "/",
		LastUpdated:  dstRoot.LastUpdated.Format(time.RFC3339),
		ParentPath:   "",
		Type:         fsservices.NodeTypeFolder,
	}

	return srcFolder, dstFolder, nil
}
