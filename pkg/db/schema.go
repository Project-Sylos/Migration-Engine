// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

const (
	tableSrcNodes   = "src_nodes"
	tableDstNodes   = "dst_nodes"
	tableSrcStats   = "src_stats"
	tableDstStats   = "dst_stats"
	tableSrcStaging = "src_staging"
	tableDstStaging = "dst_staging"
	tableMigrations = "migrations"
)

// nodeTableDDL returns CREATE TABLE for src_nodes or dst_nodes.
func nodeTableDDL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
		id VARCHAR PRIMARY KEY,
		service_id VARCHAR,
		parent_id VARCHAR,
		parent_service_id VARCHAR,
		path VARCHAR,
		parent_path VARCHAR,
		type VARCHAR,
		size BIGINT,
		mtime VARCHAR,
		depth INTEGER NOT NULL,
		traversal_status VARCHAR,
		copy_status VARCHAR,
		excluded BOOLEAN DEFAULT FALSE,
		errors VARCHAR
	)`
}

// statsTableDDL returns CREATE TABLE for stats (completed counts, etc.).
func statsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS stats (
		key VARCHAR PRIMARY KEY,
		count BIGINT NOT NULL DEFAULT 0
	)`
}

// srcStatsTableDDL returns CREATE TABLE for src_stats (traversal + copy status counts per depth).
// Keys: traversal/pending, traversal/successful, traversal/failed; copy/pending, copy/successful, copy/failed (src only).
func srcStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS src_stats (
		depth INTEGER NOT NULL,
		key VARCHAR NOT NULL,
		count BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (depth, key)
	)`
}

// dstStatsTableDDL returns CREATE TABLE for dst_stats (traversal status counts per depth).
// Keys: traversal/pending, traversal/successful, traversal/failed, traversal/not_on_src (DST only).
func dstStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS dst_stats (
		depth INTEGER NOT NULL,
		key VARCHAR NOT NULL,
		count BIGINT NOT NULL DEFAULT 0,
		PRIMARY KEY (depth, key)
	)`
}

// logsTableDDL returns CREATE TABLE for logs.
func logsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY,
		level VARCHAR,
		message VARCHAR,
		component VARCHAR,
		entity VARCHAR,
		entity_id VARCHAR,
		queue VARCHAR,
		created_at TIMESTAMP DEFAULT current_timestamp
	)`
}

// queueStatsTableDDL returns CREATE TABLE for queue_stats (queue metrics JSON).
func queueStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS queue_stats (
		queue_key VARCHAR PRIMARY KEY,
		metrics_json VARCHAR
	)`
}

// taskErrorsTableDDL returns CREATE TABLE for task_errors.
func taskErrorsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS task_errors (
		queue_type VARCHAR,
		phase VARCHAR,
		node_id VARCHAR,
		message VARCHAR,
		attempts INTEGER,
		path VARCHAR,
		created_at TIMESTAMP DEFAULT current_timestamp
	)`
}

func migrationsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS migrations (
		migration_id VARCHAR PRIMARY KEY,
		name VARCHAR NOT NULL,
		phase VARCHAR NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		service_metadata_json VARCHAR,
		root_config_json VARCHAR
	)`
}

// srcStagingDDL returns CREATE TABLE for src_staging (one row per node; new_traversal_status and new_copy_status for merge at seal).
func srcStagingDDL() string {
	return `CREATE TABLE IF NOT EXISTS src_staging (
		node_id VARCHAR PRIMARY KEY,
		new_traversal_status VARCHAR,
		new_copy_status VARCHAR
	)`
}

// dstStagingDDL returns CREATE TABLE for dst_staging (one row per node; new_traversal_status for merge at seal).
func dstStagingDDL() string {
	return `CREATE TABLE IF NOT EXISTS dst_staging (
		node_id VARCHAR PRIMARY KEY,
		new_traversal_status VARCHAR
	)`
}
