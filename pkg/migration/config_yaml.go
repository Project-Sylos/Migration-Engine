// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/configs"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"gopkg.in/yaml.v3"
)

// MigrationConfigYAML represents the complete migration configuration stored in YAML format.
type MigrationConfigYAML struct {
	Metadata         MetadataConfig         `yaml:"metadata"`
	State            StateConfig            `yaml:"state"`
	Services         ServicesConfig         `yaml:"services"`
	ServiceConfigs   map[string]any         `yaml:"service_configs,omitempty"`
	MigrationOptions MigrationOptionsConfig `yaml:"migration_options"`
	Logging          LoggingConfig          `yaml:"logging"`
	Database         DatabaseConfigYAML     `yaml:"database"`
	Verification     VerifyOptions          `yaml:"verification"`
	Extensions       map[string]any         `yaml:"extensions,omitempty"`
}

// MetadataConfig contains migration metadata.
type MetadataConfig struct {
	MigrationID  string `yaml:"migration_id"`
	CreatedAt    string `yaml:"created_at"`
	LastModified string `yaml:"last_modified"`
}

// StateConfig tracks the current migration state.
type StateConfig struct {
	Status       string `yaml:"status"` // running, paused, suspended, completed, failed
	LastLevelSrc *int   `yaml:"last_level_src,omitempty"`
	LastLevelDst *int   `yaml:"last_level_dst,omitempty"`
	LastRoundSrc *int   `yaml:"last_round_src,omitempty"`
	LastRoundDst *int   `yaml:"last_round_dst,omitempty"`
}

// ServicesConfig contains source and destination service information.
type ServicesConfig struct {
	Source         ServiceConfigYAML `yaml:"source"`
	Destination    ServiceConfigYAML `yaml:"destination"`
	SameServiceRef *SameServiceRef   `yaml:"same_service_reference,omitempty"`
}

// ServiceConfigYAML represents a single service configuration.
type ServiceConfigYAML struct {
	Type     string          `yaml:"type"`
	Name     string          `yaml:"name"`
	RootID   string          `yaml:"root_id"`
	RootPath string          `yaml:"root_path"`
	Root     *RootFolderYAML `yaml:"root,omitempty"`
}

// RootFolderYAML represents root folder information.
type RootFolderYAML struct {
	Id           string `yaml:"id"`
	ParentId     string `yaml:"parent_id"`
	ParentPath   string `yaml:"parent_path"`
	DisplayName  string `yaml:"display_name"`
	LocationPath string `yaml:"location_path"`
	LastUpdated  string `yaml:"last_updated"`
	DepthLevel   int    `yaml:"depth_level"`
	Type         string `yaml:"type"`
}

// SameServiceRef indicates when source and destination point to the same service/filesystem.
type SameServiceRef struct {
	SharedInstance bool   `yaml:"shared_instance"`
	ReferenceTo    string `yaml:"reference_to"` // "source" or "destination"
}

// MigrationOptionsConfig contains migration execution options.
type MigrationOptionsConfig struct {
	SeedRoots       bool `yaml:"seed_roots"`
	WorkerCount     int  `yaml:"worker_count"`
	MaxRetries      int  `yaml:"max_retries"`
	CoordinatorLead int  `yaml:"coordinator_lead"`
}

// LoggingConfig contains logging service configuration.
type LoggingConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
	Level   string `yaml:"level"`
}

// DatabaseConfigYAML represents database configuration in YAML.
type DatabaseConfigYAML struct {
	Path           string `yaml:"path"`
	RemoveExisting bool   `yaml:"remove_existing"`
}

// LoadMigrationConfig loads a migration configuration from a YAML file.
func LoadMigrationConfig(path string) (*MigrationConfigYAML, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg MigrationConfigYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config: %w", err)
	}

	return &cfg, nil
}

// LoadMigrationConfigFromYAML reconstructs a migration.Config from a YAML file.
// This is a convenience function that loads the YAML and converts it to a Config.
// The adapterFactory parameter is used to create FSAdapter instances.
// Example usage:
//
//	factory := func(serviceType string, serviceCfg ServiceConfigYAML, serviceConfigs map[string]any) (fsservices.FSAdapter, error) {
//	    // Your adapter creation logic here
//	}
//	cfg, err := LoadMigrationConfigFromYAML("migration.yaml", factory)
func LoadMigrationConfigFromYAML(path string, adapterFactory AdapterFactory) (Config, error) {
	yamlCfg, err := LoadMigrationConfig(path)
	if err != nil {
		return Config{}, err
	}

	cfg, err := yamlCfg.ToMigrationConfig(adapterFactory)
	if err != nil {
		return Config{}, err
	}

	// Set the config path so it can be saved later
	cfg.Database.ConfigPath = path
	cfg.ConfigPath = path

	return cfg, nil
}

// SaveMigrationConfig saves a migration configuration to a YAML file.
func SaveMigrationConfig(path string, cfg *MigrationConfigYAML) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML config: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ConfigPathFromDatabasePath returns the config YAML path for a given database path.
// Default strategy: {database_path}.yaml
func ConfigPathFromDatabasePath(dbPath string) string {
	base := dbPath
	base = strings.TrimSuffix(base, ".db")
	return base + ".yaml"
}

// AdapterFactory is a function type that creates FSAdapter instances from service configuration.
// This allows callers to provide their own adapter creation logic (e.g., for Spectra, LocalFS, etc.)
type AdapterFactory func(serviceType string, serviceCfg ServiceConfigYAML, serviceConfigs map[string]any) (fsservices.FSAdapter, error)

// ToMigrationConfig reconstructs a migration.Config from a MigrationConfigYAML.
// The adapterFactory parameter is used to create FSAdapter instances from the service configuration.
// If adapterFactory is nil, a default factory will be used that supports basic service types.
func (yamlCfg *MigrationConfigYAML) ToMigrationConfig(adapterFactory AdapterFactory) (Config, error) {
	if adapterFactory == nil {
		adapterFactory = defaultAdapterFactory
	}

	// Reconstruct source adapter
	srcAdapter, err := adapterFactory(yamlCfg.Services.Source.Type, yamlCfg.Services.Source, yamlCfg.ServiceConfigs)
	if err != nil {
		return Config{}, fmt.Errorf("failed to create source adapter: %w", err)
	}

	// Reconstruct destination adapter
	dstAdapter, err := adapterFactory(yamlCfg.Services.Destination.Type, yamlCfg.Services.Destination, yamlCfg.ServiceConfigs)
	if err != nil {
		return Config{}, fmt.Errorf("failed to create destination adapter: %w", err)
	}

	// Reconstruct root folders
	srcRoot := yamlFolderToFolder(yamlCfg.Services.Source.Root, yamlCfg.Services.Source.RootID, yamlCfg.Services.Source.RootPath)
	dstRoot := yamlFolderToFolder(yamlCfg.Services.Destination.Root, yamlCfg.Services.Destination.RootID, yamlCfg.Services.Destination.RootPath)

	// Reconstruct log address from address and port
	logAddress := fmt.Sprintf("%s:%d", yamlCfg.Logging.Address, yamlCfg.Logging.Port)

	cfg := Config{
		Database: DatabaseConfig{
			Path:           yamlCfg.Database.Path,
			RemoveExisting: yamlCfg.Database.RemoveExisting,
			ConfigPath:     "", // Will be set by caller if needed
		},
		Source: Service{
			Name:    yamlCfg.Services.Source.Name,
			Adapter: srcAdapter,
			Root:    srcRoot,
		},
		Destination: Service{
			Name:    yamlCfg.Services.Destination.Name,
			Adapter: dstAdapter,
			Root:    dstRoot,
		},
		SeedRoots:       yamlCfg.MigrationOptions.SeedRoots,
		WorkerCount:     yamlCfg.MigrationOptions.WorkerCount,
		MaxRetries:      yamlCfg.MigrationOptions.MaxRetries,
		CoordinatorLead: yamlCfg.MigrationOptions.CoordinatorLead,
		LogAddress:      logAddress,
		LogLevel:        yamlCfg.Logging.Level,
		SkipListener:    false,                  // Not stored in YAML, default to false
		StartupDelay:    2 * time.Second,        // Default
		ProgressTick:    500 * time.Millisecond, // Default
		Verification:    yamlCfg.Verification,
	}

	return cfg, nil
}

// yamlFolderToFolder converts a RootFolderYAML to fsservices.Folder.
func yamlFolderToFolder(yamlRoot *RootFolderYAML, rootID, rootPath string) fsservices.Folder {
	if yamlRoot != nil {
		return fsservices.Folder{
			Id:           yamlRoot.Id,
			ParentId:     yamlRoot.ParentId,
			ParentPath:   yamlRoot.ParentPath,
			DisplayName:  yamlRoot.DisplayName,
			LocationPath: yamlRoot.LocationPath,
			LastUpdated:  yamlRoot.LastUpdated,
			DepthLevel:   yamlRoot.DepthLevel,
			Type:         yamlRoot.Type,
		}
	}

	// Fallback to rootID and rootPath if yamlRoot is nil
	return fsservices.Folder{
		Id:           rootID,
		LocationPath: rootPath,
		Type:         fsservices.NodeTypeFolder,
		DepthLevel:   0,
	}
}

// defaultAdapterFactory provides a basic adapter factory that supports common service types.
// For Spectra and other complex services, callers should provide their own factory.
func defaultAdapterFactory(serviceType string, serviceCfg ServiceConfigYAML, serviceConfigs map[string]any) (fsservices.FSAdapter, error) {
	switch strings.ToLower(serviceType) {
	case "spectra":
		// For Spectra, we need the Spectra SDK which may not be available in this package
		// Return an error suggesting the caller provide a custom factory
		return nil, fmt.Errorf("spectra adapter requires custom AdapterFactory - use LoadMigrationConfigFromYAML with a custom factory or provide your own AdapterFactory")
	default:
		return nil, fmt.Errorf("unsupported service type: %s (provide custom AdapterFactory)", serviceType)
	}
}

// NewMigrationConfigYAML creates a new migration config YAML from a migration Config.
func NewMigrationConfigYAML(cfg Config, status MigrationStatus) (*MigrationConfigYAML, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Generate migration ID (simple hash or UUID - using timestamp for now)
	migrationID := fmt.Sprintf("migration-%d", time.Now().Unix())

	yamlCfg := &MigrationConfigYAML{
		Metadata: MetadataConfig{
			MigrationID:  migrationID,
			CreatedAt:    now,
			LastModified: now,
		},
		State: StateConfig{
			Status: "running",
		},
		Services: ServicesConfig{
			Source: ServiceConfigYAML{
				Type:     detectServiceType(cfg.Source.Adapter),
				Name:     cfg.Source.Name,
				RootID:   cfg.Source.Root.Id,
				RootPath: cfg.Source.Root.LocationPath,
				Root:     folderToYAML(cfg.Source.Root),
			},
			Destination: ServiceConfigYAML{
				Type:     detectServiceType(cfg.Destination.Adapter),
				Name:     cfg.Destination.Name,
				RootID:   cfg.Destination.Root.Id,
				RootPath: cfg.Destination.Root.LocationPath,
				Root:     folderToYAML(cfg.Destination.Root),
			},
		},
		MigrationOptions: MigrationOptionsConfig{
			SeedRoots:       cfg.SeedRoots,
			WorkerCount:     cfg.WorkerCount,
			MaxRetries:      cfg.MaxRetries,
			CoordinatorLead: cfg.CoordinatorLead,
		},
		Logging: LoggingConfig{
			Address: parseLogAddress(cfg.LogAddress),
			Port:    parseLogPort(cfg.LogAddress),
			Level:   cfg.LogLevel,
		},
		Database: DatabaseConfigYAML{
			Path:           cfg.Database.Path,
			RemoveExisting: cfg.Database.RemoveExisting,
		},
		Verification: cfg.Verification,
		Extensions:   make(map[string]any),
	}

	// Detect same service
	if isSameService(cfg.Source.Adapter, cfg.Destination.Adapter) {
		yamlCfg.Services.SameServiceRef = &SameServiceRef{
			SharedInstance: true,
			ReferenceTo:    "destination", // Default to destination
		}
	}

	// Embed service configs (especially Spectra)
	if err := embedServiceConfigs(yamlCfg, cfg); err != nil {
		return nil, fmt.Errorf("failed to embed service configs: %w", err)
	}

	// Update state from status
	if status.MinPendingDepthSrc != nil {
		level := *status.MinPendingDepthSrc
		yamlCfg.State.LastLevelSrc = &level
		yamlCfg.State.LastRoundSrc = &level
	}
	if status.MinPendingDepthDst != nil {
		level := *status.MinPendingDepthDst
		yamlCfg.State.LastLevelDst = &level
		yamlCfg.State.LastRoundDst = &level
	}

	return yamlCfg, nil
}

// UpdateConfigFromStatus updates the config YAML with current migration status.
func UpdateConfigFromStatus(yamlCfg *MigrationConfigYAML, status MigrationStatus, srcRound, dstRound int) {
	now := time.Now().UTC().Format(time.RFC3339)
	yamlCfg.Metadata.LastModified = now

	// Update rounds/levels
	yamlCfg.State.LastRoundSrc = &srcRound
	yamlCfg.State.LastLevelSrc = &srcRound
	yamlCfg.State.LastRoundDst = &dstRound
	yamlCfg.State.LastLevelDst = &dstRound

	// Update status (only if not already set to "suspended" by force shutdown)
	if yamlCfg.State.Status != "suspended" {
		if status.IsComplete() {
			yamlCfg.State.Status = "completed"
		} else if status.HasPending() {
			yamlCfg.State.Status = "running"
		} else if status.HasFailures() {
			yamlCfg.State.Status = "failed"
		}
	}
}

// SetSuspendedStatus sets the YAML config status to "suspended" with current state.
// This is used during force shutdown to mark the migration as suspended for resumption.
func SetSuspendedStatus(yamlCfg *MigrationConfigYAML, status MigrationStatus, srcRound, dstRound int) {
	now := time.Now().UTC().Format(time.RFC3339)
	yamlCfg.Metadata.LastModified = now

	// Update rounds/levels
	yamlCfg.State.LastRoundSrc = &srcRound
	yamlCfg.State.LastLevelSrc = &srcRound
	yamlCfg.State.LastRoundDst = &dstRound
	yamlCfg.State.LastLevelDst = &dstRound

	// Set status to suspended
	yamlCfg.State.Status = "suspended"
}

// UpdateConfigFromRoots updates the config YAML when roots are set.
func UpdateConfigFromRoots(yamlCfg *MigrationConfigYAML, srcRoot, dstRoot fsservices.Folder) {
	now := time.Now().UTC().Format(time.RFC3339)
	yamlCfg.Metadata.LastModified = now

	yamlCfg.Services.Source.RootID = srcRoot.Id
	yamlCfg.Services.Source.RootPath = srcRoot.LocationPath
	yamlCfg.Services.Source.Root = folderToYAML(srcRoot)

	yamlCfg.Services.Destination.RootID = dstRoot.Id
	yamlCfg.Services.Destination.RootPath = dstRoot.LocationPath
	yamlCfg.Services.Destination.Root = folderToYAML(dstRoot)
}

// Helper functions

func detectServiceType(adapter fsservices.FSAdapter) string {
	if adapter == nil {
		return "unknown"
	}

	// Type assertion to detect Spectra
	if _, ok := adapter.(*fsservices.SpectraFS); ok {
		return "spectra"
	}

	// Type assertion to detect LocalFS (if it exists)
	adapterType := reflect.TypeOf(adapter).String()
	if adapterType == "*fsservices.LocalFS" {
		return "local"
	}

	// Default to type name
	return adapterType
}

func isSameService(srcAdapter, dstAdapter fsservices.FSAdapter) bool {
	if srcAdapter == nil || dstAdapter == nil {
		return false
	}

	// Check if both are SpectraFS
	_, srcOk := srcAdapter.(*fsservices.SpectraFS)
	_, dstOk := dstAdapter.(*fsservices.SpectraFS)

	if srcOk && dstOk {
		// For Spectra, check if they share the same instance
		// We can't directly compare the internal fs pointer, but we can check
		// if they're the same type and have similar characteristics
		// For now, assume same service if both are Spectra
		return true
	}

	// Check if both are LocalFS pointing to same filesystem
	// This would require additional logic to compare paths

	return false
}

func folderToYAML(folder fsservices.Folder) *RootFolderYAML {
	return &RootFolderYAML{
		Id:           folder.Id,
		ParentId:     folder.ParentId,
		ParentPath:   folder.ParentPath,
		DisplayName:  folder.DisplayName,
		LocationPath: folder.LocationPath,
		LastUpdated:  folder.LastUpdated,
		DepthLevel:   folder.DepthLevel,
		Type:         folder.Type,
	}
}

func embedServiceConfigs(yamlCfg *MigrationConfigYAML, cfg Config) error {
	yamlCfg.ServiceConfigs = make(map[string]any)

	// Check if Spectra is used
	srcIsSpectra := isSpectraAdapter(cfg.Source.Adapter)
	dstIsSpectra := isSpectraAdapter(cfg.Destination.Adapter)

	if srcIsSpectra || dstIsSpectra {
		// Load spectra.json directly (it's not wrapped)
		spectraPath := "pkg/configs/spectra.json"
		data, readErr := os.ReadFile(spectraPath)
		if readErr != nil {
			// If default path fails, try to get from config
			spectraConfig, configErr := configs.LoadSpectraConfig("pkg/configs")
			if configErr == nil && spectraConfig.ConfigPath != "" {
				spectraPath = spectraConfig.ConfigPath
				data, readErr = os.ReadFile(spectraPath)
			}
			if readErr != nil {
				// If we can't read the file, create a minimal config
				yamlCfg.ServiceConfigs["spectra"] = map[string]any{
					"note": "spectra config file not found, using defaults",
				}
				return nil
			}
		}

		var spectraData map[string]any
		if jsonErr := json.Unmarshal(data, &spectraData); jsonErr != nil {
			return fmt.Errorf("failed to parse spectra config: %w", jsonErr)
		}

		yamlCfg.ServiceConfigs["spectra"] = spectraData
	}

	return nil
}

func isSpectraAdapter(adapter fsservices.FSAdapter) bool {
	if adapter == nil {
		return false
	}
	_, ok := adapter.(*fsservices.SpectraFS)
	return ok
}

func parseLogAddress(logAddress string) string {
	if logAddress == "" {
		return "127.0.0.1"
	}
	// Parse address:port format
	// Split by colon, take the address part
	for i := len(logAddress) - 1; i >= 0; i-- {
		if logAddress[i] == ':' {
			return logAddress[:i]
		}
	}
	// No colon found, return as-is
	return logAddress
}

func parseLogPort(logAddress string) int {
	// Default port
	if logAddress == "" {
		return 8081
	}
	// Try to parse port from address:port format
	for i := len(logAddress) - 1; i >= 0; i-- {
		if logAddress[i] == ':' {
			var port int
			if _, err := fmt.Sscanf(logAddress[i+1:], "%d", &port); err == nil {
				return port
			}
			break
		}
	}
	// No colon or invalid port, return default
	return 8081
}
