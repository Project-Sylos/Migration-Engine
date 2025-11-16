// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
)

type folderConfig struct {
	Id           string `json:"id"`
	ParentId     string `json:"parentId"`
	ParentPath   string `json:"parentPath"`
	DisplayName  string `json:"displayName"`
	LocationPath string `json:"locationPath"`
	LastUpdated  string `json:"lastUpdated"`
	DepthLevel   int    `json:"depthLevel"`
	Type         string `json:"type"`
}

type adapterConfig struct {
	Type          string       `json:"type"`
	Name          string       `json:"name"`
	RootID        string       `json:"rootId"`
	World         string       `json:"world"`
	SpectraConfig string       `json:"spectraConfig"`
	Root          folderConfig `json:"root"`
}

type cliConfig struct {
	Database           migration.DatabaseConfig `json:"database"`
	Source             adapterConfig            `json:"source"`
	Destination        adapterConfig            `json:"destination"`
	SeedRoots          bool                     `json:"seedRoots"`
	WorkerCount        int                      `json:"workerCount"`
	MaxRetries         int                      `json:"maxRetries"`
	CoordinatorLead    int                      `json:"coordinatorLead"`
	LogAddress         string                   `json:"logAddress"`
	LogLevel           string                   `json:"logLevel"`
	SkipListener       bool                     `json:"skipListener"`
	StartupDelayMillis int                      `json:"startupDelayMillis"`
	ProgressTickMillis int                      `json:"progressTickMillis"`
	Verification       migration.VerifyOptions  `json:"verification"`
}

type adapterFactory struct {
	spectra map[string]*sdk.SpectraFS
}

func newAdapterFactory() *adapterFactory {
	return &adapterFactory{spectra: make(map[string]*sdk.SpectraFS)}
}

func (f *adapterFactory) spectraInstance(path string) (*sdk.SpectraFS, error) {
	if fs := f.spectra[path]; fs != nil {
		return fs, nil
	}

	fs, err := sdk.New(path)
	if err != nil {
		return nil, err
	}

	f.spectra[path] = fs
	return fs, nil
}

func (f *folderConfig) toFolder(defaultName string) (fsservices.Folder, bool, error) {
	if f == nil {
		return fsservices.Folder{}, false, nil
	}
	if f.Id == "" {
		return fsservices.Folder{}, false, nil
	}

	folder := fsservices.Folder{
		Id:           f.Id,
		ParentId:     f.ParentId,
		ParentPath:   f.ParentPath,
		DisplayName:  f.DisplayName,
		LocationPath: f.LocationPath,
		LastUpdated:  f.LastUpdated,
		DepthLevel:   f.DepthLevel,
		Type:         f.Type,
	}

	if folder.DisplayName == "" {
		folder.DisplayName = defaultName
	}
	if folder.LocationPath == "" {
		folder.LocationPath = "/"
	}
	if folder.Type == "" {
		folder.Type = fsservices.NodeTypeFolder
	}
	if folder.LastUpdated == "" {
		folder.LastUpdated = time.Now().UTC().Format(time.RFC3339)
	}

	return folder, true, nil
}

func (f *adapterFactory) buildService(cfg adapterConfig) (migration.Service, fsservices.Folder, error) {
	var folder fsservices.Folder
	var hasFolder bool
	var err error
	if cfg.Root.Id != "" {
		folder, hasFolder, err = cfg.Root.toFolder(cfg.Root.Id)
		if err != nil {
			return migration.Service{}, fsservices.Folder{}, err
		}
	}

	switch strings.ToLower(cfg.Type) {
	case "spectra":
		configPath := cfg.SpectraConfig
		if configPath == "" {
			configPath = "pkg/configs/spectra.json"
		}

		spectraFS, err := f.spectraInstance(configPath)
		if err != nil {
			return migration.Service{}, fsservices.Folder{}, fmt.Errorf("spectra init failed: %w", err)
		}

		rootID := cfg.RootID
		if rootID == "" {
			if hasFolder {
				rootID = folder.Id
			} else {
				rootID = "root"
			}
		}

		world := cfg.World
		if world == "" {
			world = "primary"
		}

		adapter, err := fsservices.NewSpectraFS(spectraFS, rootID, world)
		if err != nil {
			return migration.Service{}, fsservices.Folder{}, fmt.Errorf("spectra adapter: %w", err)
		}

		if !hasFolder {
			node, err := spectraFS.GetNode(&sdk.GetNodeRequest{ID: rootID})
			if err != nil {
				return migration.Service{}, fsservices.Folder{}, fmt.Errorf("spectra get node %s: %w", rootID, err)
			}
			if node.Type != fsservices.NodeTypeFolder {
				return migration.Service{}, fsservices.Folder{}, fmt.Errorf("spectra node %s is not a folder", rootID)
			}

			folder = fsservices.Folder{
				Id:           node.ID,
				DisplayName:  node.Name,
				LocationPath: "/",
				LastUpdated:  node.LastUpdated.Format(time.RFC3339),
				ParentId:     "",
				ParentPath:   "",
				DepthLevel:   0,
				Type:         fsservices.NodeTypeFolder,
			}
		}

		name := cfg.Name
		if name == "" {
			name = fmt.Sprintf("Spectra-%s", strings.ToUpper(world))
		}

		return migration.Service{Name: name, Adapter: adapter}, folder, nil
	default:
		return migration.Service{}, fsservices.Folder{}, fmt.Errorf("unsupported adapter type %q", cfg.Type)
	}
}

func main() {
	configPath := flag.String("config", "", "Path to migration configuration JSON")
	flag.Parse()

	if *configPath == "" {
		fmt.Println("Usage: Migration-Engine -config path/to/config.json")
		os.Exit(1)
	}

	cfg, err := loadCLIConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	migrationCfg, err := buildMigrationConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build migration config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("=== Migration Engine ===")
	result, err := migration.LetsMigrate(migrationCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}

	printResult(result)
}

func loadCLIConfig(path string) (cliConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return cliConfig{}, err
	}

	var cfg cliConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cliConfig{}, err
	}

	return cfg, nil
}

func buildMigrationConfig(cfg cliConfig) (migration.Config, error) {
	factory := newAdapterFactory()

	sourceService, srcRoot, err := factory.buildService(cfg.Source)
	if err != nil {
		return migration.Config{}, fmt.Errorf("source: %w", err)
	}

	destinationService, dstRoot, err := factory.buildService(cfg.Destination)
	if err != nil {
		return migration.Config{}, fmt.Errorf("destination: %w", err)
	}

	// Default verification behavior:
	// If verification options are omitted in the CLI config (zero-value struct),
	// we treat "AllowNotOnSrc" as true by default so that extra items on the
	// destination do not fail verification unless explicitly disallowed.
	verifyOpts := cfg.Verification
	if verifyOpts == (migration.VerifyOptions{}) {
		verifyOpts.AllowNotOnSrc = true
	}

	migrationCfg := migration.Config{
		Database:        cfg.Database,
		Source:          sourceService,
		Destination:     destinationService,
		SeedRoots:       cfg.SeedRoots,
		WorkerCount:     cfg.WorkerCount,
		MaxRetries:      cfg.MaxRetries,
		CoordinatorLead: cfg.CoordinatorLead,
		LogAddress:      cfg.LogAddress,
		LogLevel:        cfg.LogLevel,
		SkipListener:    cfg.SkipListener,
		Verification:    verifyOpts,
	}

	if cfg.StartupDelayMillis > 0 {
		migrationCfg.StartupDelay = time.Duration(cfg.StartupDelayMillis) * time.Millisecond
	}
	if cfg.ProgressTickMillis > 0 {
		migrationCfg.ProgressTick = time.Duration(cfg.ProgressTickMillis) * time.Millisecond
	}

	if err := migrationCfg.SetRootFolders(srcRoot, dstRoot); err != nil {
		return migration.Config{}, err
	}

	return migrationCfg, nil
}

func printResult(result migration.Result) {
	report := result.Verification

	fmt.Println()
	fmt.Println("=== Migration Summary ===")
	fmt.Printf("Duration: %s\n", result.Runtime.Duration)
	if result.RootsSeeded {
		fmt.Printf("Root tasks seeded: src=%d dst=%d\n", result.RootSummary.SrcRoots, result.RootSummary.DstRoots)
	} else {
		fmt.Println("Root task seeding skipped (assumed pre-seeded)")
	}
	fmt.Printf("Final src queue round: %d\n", result.Runtime.Src.Round)
	fmt.Printf("Final dst queue round: %d\n", result.Runtime.Dst.Round)

	fmt.Println()
	fmt.Println("Traversal status:")
	fmt.Printf("  Src: %d Successful, %d Pending, %d Failed\n",
		report.SrcTotal-report.SrcPending-report.SrcFailed,
		report.SrcPending,
		report.SrcFailed,
	)
	fmt.Printf("  Dst: %d Successful, %d Pending, %d Failed\n",
		report.DstTotal-report.DstPending-report.DstFailed,
		report.DstPending,
		report.DstFailed,
	)

	fmt.Println()
	fmt.Printf("✓ Successfully migrated %d nodes\n", report.SrcTotal)
}
