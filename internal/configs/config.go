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
	BatchSize          int           `json:"batch_size"`
	FlushIntervalSec   int           `json:"flush_interval_seconds"`
}

// BufferConfig maps table names to their configs.
type BufferConfig map[string]BufferTableConfig

// LoadBufferConfig reads internal/configs/buffers.json.
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

// LoadLogServiceConfig reads internal/configs/log_service.json.
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
