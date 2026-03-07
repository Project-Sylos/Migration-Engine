// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

const (
	tableSrcNodes         = "src_nodes"
	tableDstNodes         = "dst_nodes"
	tableSrcStatusEvents  = "src_status_events"
	tableDstStatusEvents  = "dst_status_events"
	tableSrcStats         = "src_stats"
	tableDstStats         = "dst_stats"
	tableStats            = "stats" // universal key/count table for canonical review stats
	// TableMigrations is the migrations lifecycle table name (used by schema and migration store).
	TableMigrations = "migrations"
)

// nodeTableDDL returns CREATE TABLE for src_nodes or dst_nodes (metadata only; no status columns).
func nodeTableDDL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
		id VARCHAR PRIMARY KEY,
		service_id VARCHAR,
		parent_id VARCHAR,
		parent_service_id VARCHAR,
		path VARCHAR,
		parent_path VARCHAR,
		path_hash VARCHAR,
		parent_path_hash VARCHAR,
		type VARCHAR,
		size BIGINT,
		mtime VARCHAR,
		depth INTEGER NOT NULL
	)`
}

// srcStatusEventsTableDDL returns CREATE TABLE for append-only source status events.
func srcStatusEventsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + tableSrcStatusEvents + ` (
		id VARCHAR NOT NULL,
		traversal_status VARCHAR,
		copy_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL
	)`
}

// dstStatusEventsTableDDL returns CREATE TABLE for append-only destination status events.
func dstStatusEventsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + tableDstStatusEvents + ` (
		id VARCHAR NOT NULL,
		traversal_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL
	)`
}

// statsTableDDL returns CREATE TABLE for the universal stats table (key/count for review and other global counters).
func statsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + tableStats + ` (
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
		id VARCHAR PRIMARY KEY,
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
	return `CREATE TABLE IF NOT EXISTS ` + TableMigrations + ` (
		migration_id VARCHAR PRIMARY KEY,
		name VARCHAR NOT NULL,
		phase VARCHAR NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		service_metadata_json VARCHAR,
		root_config_json VARCHAR
	)`
}

