// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

type SrcNodesTable struct{}

func (t SrcNodesTable) Name() string {
	return "src_nodes"
}

/*
Note: For now we are using the path as the primary key / foreign key connectors
between the two node trees. In later editions we may modify this.
Secondary note: Path is just the path relative to the root, not the full
absolute path. Since root path relativity should be functionally the same
across node trees.
*/

func (t SrcNodesTable) Schema() string {
	return `
		id VARCHAR NOT NULL UNIQUE,
		parent_id VARCHAR NOT NULL,
		name VARCHAR NOT NULL,
		path VARCHAR NOT NULL UNIQUE,
		parent_path VARCHAR NOT NULL,
		type VARCHAR NOT NULL,
		depth_level INTEGER NOT NULL,
		size BIGINT,
		last_updated TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('Missing', 'Pending', 'Successful', 'Failed')),
		copy_status VARCHAR NOT NULL CHECK(copy_status IN ('Missing', 'Pending', 'Successful', 'Failed'))
	`
}

type DstNodesTable struct{}

func (t DstNodesTable) Name() string {
	return "dst_nodes"
}

/*
Note: You might be wondering why src nodes table has an copy status field but
dst does not. That's because the action of moving one node from another is a
coupled action.
What this means in practice is that in order to copy from one node to another
we have to take a chunk of data from that node on src, move it to dst, and
rinse and repeat until we've moved everything for that node. This means it is
functionally impossible to consider one src / dst node successful without the
other. Thus storing it just in one table is sufficient.
*/

func (t DstNodesTable) Schema() string {
	return `
		id VARCHAR NOT NULL UNIQUE,
		parent_id VARCHAR NOT NULL,
		name VARCHAR NOT NULL,
		path VARCHAR NOT NULL UNIQUE,
		parent_path VARCHAR NOT NULL,
		type VARCHAR NOT NULL,
		depth_level INTEGER NOT NULL,
		size BIGINT,
		last_updated TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL CHECK(traversal_status IN ('Missing', 'NotOnSrc', 'Pending', 'Successful', 'Failed'))
	`
}

type LogsTable struct{}

func (t LogsTable) Name() string {
	return "logs"
}

// Schema returns the schema definition.
func (t LogsTable) Schema() string {
	return `
		id VARCHAR PRIMARY KEY,
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		level VARCHAR NOT NULL CHECK(level IN ('trace', 'debug', 'info', 'warning', 'error', 'critical')),
		entity VARCHAR DEFAULT NULL,
		entity_id VARCHAR DEFAULT NULL,
		details VARCHAR DEFAULT NULL,
		message VARCHAR NOT NULL,
		queue VARCHAR DEFAULT NULL
	`
}

// Init creates the audit log table if it doesn't exist.
func (t LogsTable) Init(db *DB) error {
	return db.CreateTable(t.Name(), t.Schema())
}
