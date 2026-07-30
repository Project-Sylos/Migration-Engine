// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

const (
	TableSrcNodes             = "src_nodes"
	TableDstNodes             = "dst_nodes"
	TableSrcStatusEvents      = "src_status_events"
	TableDstStatusEvents      = "dst_status_events"
	TableGPLIssues            = "gpl_issues"
	TableIDMap                = "id_map"
	TableSrcCurrent           = "src_current"
	TableDstCurrent           = "dst_current"
	TableSrcStats             = "src_stats"
	TableDstStats             = "dst_stats"
	TableStats                = "stats" // universal key/count table for canonical review stats
	TableCopyWorkRoundStats   = "copy_work_round_stats"
	TableDeleteWorkRoundStats = "delete_work_round_stats"
	// TableMigrations is the migrations lifecycle table name (used by schema and migration store).
	TableMigrations = "migrations"
	// TableFSCredentialBinding holds per-side connection id, optional creds file path, and serialized root folder for API rehydration.
	TableFSCredentialBinding = "fs_credential_binding"
	// TableOAuthCredentials stores OAuth refresh token JSON keyed by connection id.
	// Values may be plaintext (tests) or AES-GCM blobs prefixed with enc:v1: (production).
	TableOAuthCredentials = "oauth_credentials"
)

// nodeTableDDL returns CREATE TABLE for src_nodes or dst_nodes (metadata only; no status columns).
// src_nodes includes nullable transfer checkpoint columns (xfer_*) and gpl_state; dst_nodes does not.
// name is last in both tables so ADD COLUMN on an older DB yields the same physical layout
// as a fresh CREATE (the appender writes values positionally).
func nodeTableDDL(table string) string {
	base := `CREATE TABLE IF NOT EXISTS ` + table + ` (
		id VARCHAR PRIMARY KEY,
		service_id VARCHAR,
		parent_id VARCHAR,
		parent_service_id VARCHAR,
		path VARCHAR,
		parent_path VARCHAR,
		type VARCHAR,
		size BIGINT,
		mtime VARCHAR,
		depth INTEGER NOT NULL`
	if table == TableSrcNodes {
		base += `,
		` + colXferOffset + ` BIGINT,
		` + colXferSrcSize + ` BIGINT,
		` + colXferSrcMTime + ` VARCHAR,
		` + colXferDstRef + ` VARCHAR,
		gpl_state VARCHAR`
	}
	return base + `,
		` + colName + ` VARCHAR
	)`
}

// colName is the stored display basename (provider-reported), used by review search,
// name sort, and destination child matching instead of re-deriving it from path.
const colName = "name"

func nodeTableNameAlters() []string {
	return []string{
		`ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS ` + colName + ` VARCHAR`,
		`ALTER TABLE ` + TableDstNodes + ` ADD COLUMN IF NOT EXISTS ` + colName + ` VARCHAR`,
	}
}

// Transfer checkpoint columns are nullable; copy_status stays pending while a checkpoint exists.
// ensureSrcTransferCheckpointColumns ALTERs older DBs that were created before these columns existed.
const (
	ColXferOffset   = "xfer_offset"
	ColXferSrcSize  = "xfer_src_size"
	ColXferSrcMTime = "xfer_src_mtime"
	ColXferDstRef   = "xfer_dst_ref"
)

// Deprecated aliases kept for any in-package references during the split.
const (
	colXferOffset   = ColXferOffset
	colXferSrcSize  = ColXferSrcSize
	colXferSrcMTime = ColXferSrcMTime
	colXferDstRef   = ColXferDstRef
)

func srcNodesTransferCheckpointAlters() []string {
	return []string{
		`ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS ` + colXferOffset + ` BIGINT`,
		`ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS ` + colXferSrcSize + ` BIGINT`,
		`ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS ` + colXferSrcMTime + ` VARCHAR`,
		`ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS ` + colXferDstRef + ` VARCHAR`,
	}
}

// srcStatusEventsTableDDL returns CREATE TABLE for append-only source status events.
func srcStatusEventsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableSrcStatusEvents + ` (
		id VARCHAR NOT NULL,
		traversal_status VARCHAR,
		copy_status VARCHAR,
		delete_status VARCHAR,
		gpl_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL,
		error_log_id VARCHAR
	)`
}

// dstStatusEventsTableDDL returns CREATE TABLE for append-only destination status events.
func dstStatusEventsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableDstStatusEvents + ` (
		id VARCHAR NOT NULL,
		traversal_status VARCHAR,
		gpl_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL,
		error_log_id VARCHAR
	)`
}

// statsTableDDL returns CREATE TABLE for the universal stats table (key/count for review and other global counters).
func statsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableStats + ` (
		key VARCHAR PRIMARY KEY,
		count BIGINT NOT NULL DEFAULT 0
	)`
}

// srcStatsTableDDL returns CREATE TABLE for src_stats (traversal + copy status counts per depth).
// Keys: traversal/pending, traversal/successful, traversal/failed; copy/pending, copy/successful, copy/failed; delete/pending, delete/deleted, delete/failed (src only).
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
		detail VARCHAR,
		component VARCHAR,
		entity VARCHAR,
		entity_id VARCHAR,
		queue VARCHAR,
		created_at TIMESTAMP DEFAULT current_timestamp
	)`
}

// queueStatsTableDDL returns CREATE TABLE for queue_stats (append-only queue metrics JSON).
func queueStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS queue_stats (
		queue_key VARCHAR NOT NULL,
		phase VARCHAR NOT NULL,
		event_time TIMESTAMP NOT NULL DEFAULT current_timestamp,
		metrics_json VARCHAR NOT NULL
	)`
}

// copyWorkRoundStatsTableDDL returns CREATE TABLE for append-only copy-work depth seals.
// Rows are deltas (folders/files/bytes) credited when a depth is finalized after DST matching.
func copyWorkRoundStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableCopyWorkRoundStats + ` (
		depth INTEGER NOT NULL,
		folders BIGINT NOT NULL DEFAULT 0,
		files BIGINT NOT NULL DEFAULT 0,
		bytes BIGINT NOT NULL DEFAULT 0,
		reason VARCHAR NOT NULL DEFAULT '',
		sealed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
	)`
}

// deleteWorkRoundStatsTableDDL returns CREATE TABLE for append-only delete-work seals
// (typically one snapshot at delete-phase start; retries append nothing if absolute unchanged).
func deleteWorkRoundStatsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableDeleteWorkRoundStats + ` (
		depth INTEGER NOT NULL,
		folders BIGINT NOT NULL DEFAULT 0,
		files BIGINT NOT NULL DEFAULT 0,
		bytes BIGINT NOT NULL DEFAULT 0,
		reason VARCHAR NOT NULL DEFAULT '',
		sealed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
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
		root_config_json VARCHAR,
		runtime_state_json VARCHAR
	)`
}

func fsCredentialBindingTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableFSCredentialBinding + ` (
		role VARCHAR PRIMARY KEY,
		connection_id VARCHAR NOT NULL DEFAULT '',
		creds_conf_relpath VARCHAR,
		service_id VARCHAR,
		root_folder_json VARCHAR,
		updated_at TIMESTAMP NOT NULL
	)`
}

func oauthCredentialsTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableOAuthCredentials + ` (
		connection_id VARCHAR PRIMARY KEY,
		creds_json VARCHAR NOT NULL,
		updated_at TIMESTAMP NOT NULL
	)`
}

// gplIssuesTableDDL is the sparse, current GPL review queue. Hot-path writers
// only insert rows; review mutations update them in place.
func gplIssuesTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableGPLIssues + ` (
		src_id VARCHAR PRIMARY KEY,
		status VARCHAR NOT NULL,
		proposed_name VARCHAR,
		issues_json VARCHAR,
		updated_at BIGINT NOT NULL
	)`
}

func gplIssuesDstActionAlter() string {
	return `ALTER TABLE ` + TableGPLIssues + ` ADD COLUMN IF NOT EXISTS dst_action VARCHAR`
}

// idMapTableDDL returns CREATE TABLE for append-only SRC↔DST identity mappings.
func idMapTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableIDMap + ` (
		src_internal_id VARCHAR NOT NULL,
		dst_internal_id VARCHAR NOT NULL,
		event_time BIGINT NOT NULL,
		source VARCHAR NOT NULL,
		status VARCHAR NOT NULL
	)`
}

// srcCurrentTableDDL is the review read model: one row per SRC id (latest status).
func srcCurrentTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableSrcCurrent + ` (
		id VARCHAR PRIMARY KEY,
		traversal_status VARCHAR,
		copy_status VARCHAR,
		delete_status VARCHAR,
		error_log_id VARCHAR,
		gpl_status VARCHAR,
		resolved_dst_name VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL
	)`
}

// dstCurrentTableDDL is the review read model: one row per DST id (latest status).
func dstCurrentTableDDL() string {
	return `CREATE TABLE IF NOT EXISTS ` + TableDstCurrent + ` (
		id VARCHAR PRIMARY KEY,
		traversal_status VARCHAR,
		error_log_id VARCHAR,
		gpl_status VARCHAR,
		event_time BIGINT NOT NULL,
		depth INTEGER NOT NULL
	)`
}

func srcNodesGPLStateAlter() string {
	return `ALTER TABLE ` + TableSrcNodes + ` ADD COLUMN IF NOT EXISTS gpl_state VARCHAR`
}

func statusEventsGPLStatusAlters() []string {
	return []string{
		`ALTER TABLE ` + TableSrcStatusEvents + ` ADD COLUMN IF NOT EXISTS gpl_status VARCHAR`,
		`ALTER TABLE ` + TableDstStatusEvents + ` ADD COLUMN IF NOT EXISTS gpl_status VARCHAR`,
	}
}
