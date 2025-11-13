// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package configs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// BufferTableConfig represents one table's buffer settings.
type BufferTableConfig struct {
	BatchSize        int `json:"batch_size"`
	FlushIntervalSec int `json:"flush_interval_seconds"`
}

// BufferConfig maps table names to their configs.
type BufferConfig map[string]BufferTableConfig

// LoadBufferConfig reads pkg/configs/buffers.json.
func LoadBufferConfig(configDir string) (BufferConfig, error) {
	path := filepath.Join(configDir, "buffers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read buffer config: %w", err)
	}

	var cfg BufferConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse buffer config: %w", err)
	}
	return cfg, nil
}

// LogServiceConfig holds UDP logger settings.
type LogServiceConfig struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
	Level   string `json:"level"`
}

// LoadLogServiceConfig reads pkg/configs/log_service.json.
func LoadLogServiceConfig(configDir string) (LogServiceConfig, error) {
	path := filepath.Join(configDir, "log_service.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return LogServiceConfig{}, fmt.Errorf("failed to read log service config: %w", err)
	}

	var cfg LogServiceConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return LogServiceConfig{}, fmt.Errorf("failed to parse log service config: %w", err)
	}
	return cfg, nil
}

// SpectraConfig holds Spectra filesystem settings.
type SpectraConfig struct {
	ConfigPath     string `json:"config_path"`
	SrcRootID      string `json:"src_root_id"`
	DstRootID      string `json:"dst_root_id"`
	SharedInstance bool   `json:"shared_instance"`
	Description    string `json:"description"`
}

// SpectraConfigWrapper wraps the Spectra configuration.
type SpectraConfigWrapper struct {
	Spectra SpectraConfig `json:"spectra"`
}

// LoadSpectraConfig reads pkg/configs/spectra.json.
func LoadSpectraConfig(configDir string) (SpectraConfig, error) {
	path := filepath.Join(configDir, "spectra.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return SpectraConfig{}, fmt.Errorf("failed to read spectra config: %w", err)
	}

	var wrapper SpectraConfigWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return SpectraConfig{}, fmt.Errorf("failed to parse spectra config: %w", err)
	}
	return wrapper.Spectra, nil
}
