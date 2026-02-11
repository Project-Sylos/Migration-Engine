// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

// AppenderWriter holds DuckDB appenders for staging and node tables. Use inside RunAppenderTx.
// Order of use: append all src_staging, then dst_staging, then src_nodes, then dst_nodes (do not interleave).
type AppenderWriter struct {
	srcStaging *duckdb.Appender
	dstStaging *duckdb.Appender
	srcNodes   *duckdb.Appender
	dstNodes   *duckdb.Appender
}

// newAppenderWriter creates appenders from the given driver connection. Call from inside conn.Raw().
func newAppenderWriter(driverConn driver.Conn) (*AppenderWriter, error) {
	aw := &AppenderWriter{}
	var err error
	aw.srcStaging, err = duckdb.NewAppenderFromConn(driverConn, "", tableSrcStaging)
	if err != nil {
		return nil, err
	}
	aw.dstStaging, err = duckdb.NewAppenderFromConn(driverConn, "", tableDstStaging)
	if err != nil {
		_ = aw.srcStaging.Close()
		return nil, err
	}
	aw.srcNodes, err = duckdb.NewAppenderFromConn(driverConn, "", tableSrcNodes)
	if err != nil {
		_ = aw.srcStaging.Close()
		_ = aw.dstStaging.Close()
		return nil, err
	}
	aw.dstNodes, err = duckdb.NewAppenderFromConn(driverConn, "", tableDstNodes)
	if err != nil {
		_ = aw.srcStaging.Close()
		_ = aw.dstStaging.Close()
		_ = aw.srcNodes.Close()
		return nil, err
	}
	return aw, nil
}

// AppendNode appends one row to src_nodes or dst_nodes. table is "SRC" or "DST". Order of columns matches schema.
func (aw *AppenderWriter) AppendNode(table string, n *NodeState) error {
	traversalStatus := n.TraversalStatus
	if traversalStatus == "" {
		traversalStatus = n.Status
	}
	rowArgs := []driver.Value{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath,
		n.Type, n.Size, n.MTime, int64(n.Depth), traversalStatus, n.CopyStatus, n.Excluded, n.Errors,
	}
	switch table {
	case "DST":
		return aw.dstNodes.AppendRow(rowArgs...)
	default:
		return aw.srcNodes.AppendRow(rowArgs...)
	}
}

// Flush flushes all appenders so data is written to the table. Call before Close/commit.
func (aw *AppenderWriter) Flush() error {
	if aw.srcStaging != nil {
		if err := aw.srcStaging.Flush(); err != nil {
			return err
		}
	}
	if aw.dstStaging != nil {
		if err := aw.dstStaging.Flush(); err != nil {
			return err
		}
	}
	if aw.srcNodes != nil {
		if err := aw.srcNodes.Flush(); err != nil {
			return err
		}
	}
	if aw.dstNodes != nil {
		if err := aw.dstNodes.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all appenders. Call after Flush.
func (aw *AppenderWriter) Close() error {
	var err error
	if aw.srcStaging != nil {
		err = aw.srcStaging.Close()
		aw.srcStaging = nil
	}
	if aw.dstStaging != nil {
		if e := aw.dstStaging.Close(); e != nil {
			err = e
		}
		aw.dstStaging = nil
	}
	if aw.srcNodes != nil {
		if e := aw.srcNodes.Close(); e != nil {
			err = e
		}
		aw.srcNodes = nil
	}
	if aw.dstNodes != nil {
		if e := aw.dstNodes.Close(); e != nil {
			err = e
		}
		aw.dstNodes = nil
	}
	return err
}
