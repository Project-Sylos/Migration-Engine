// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

// queueAppenderWriter holds appenders for a single queue (SRC or DST).
// SRC: srcStaging + srcNodes. DST: dstStaging + dstNodes + srcStaging (for copy updates).
// Implements appenderFlusher for staging and node flushes.
type queueAppenderWriter struct {
	srcStaging *duckdb.Appender
	dstStaging *duckdb.Appender
	srcNodes   *duckdb.Appender
	dstNodes   *duckdb.Appender
	queueType  string
	hasNodes   bool
}

// newQueueAppenderWriter creates appenders for the given queue. nodesIncluded=true creates node appenders.
func newQueueAppenderWriter(driverConn driver.Conn, queueType string, nodesIncluded bool) (*queueAppenderWriter, error) {
	aw := &queueAppenderWriter{queueType: queueType, hasNodes: nodesIncluded}
	var err error

	if queueType == "SRC" {
		aw.srcStaging, err = duckdb.NewAppenderFromConn(driverConn, "", tableSrcStaging)
		if err != nil {
			return nil, err
		}
		if nodesIncluded {
			aw.srcNodes, err = duckdb.NewAppenderFromConn(driverConn, "", tableSrcNodes)
			if err != nil {
				_ = aw.srcStaging.Close()
				return nil, err
			}
		}
		return aw, nil
	}

	// DST: dstStaging, dstNodes, srcStaging (for copy updates from DST)
	aw.srcStaging, err = duckdb.NewAppenderFromConn(driverConn, "", tableSrcStaging)
	if err != nil {
		return nil, err
	}
	aw.dstStaging, err = duckdb.NewAppenderFromConn(driverConn, "", tableDstStaging)
	if err != nil {
		_ = aw.srcStaging.Close()
		return nil, err
	}
	if nodesIncluded {
		aw.dstNodes, err = duckdb.NewAppenderFromConn(driverConn, "", tableDstNodes)
		if err != nil {
			_ = aw.srcStaging.Close()
			_ = aw.dstStaging.Close()
			return nil, err
		}
	}
	return aw, nil
}

func (aw *queueAppenderWriter) appendSrcStaging(nodeID, traversal, copySt string) error {
	if aw.srcStaging == nil {
		return nil
	}
	return aw.srcStaging.AppendRow(nodeID, traversal, copySt)
}

func (aw *queueAppenderWriter) appendDstStaging(nodeID, newTraversal string) error {
	if aw.dstStaging == nil {
		return nil
	}
	return aw.dstStaging.AppendRow(nodeID, newTraversal)
}

func (aw *queueAppenderWriter) appendNode(table string, n *NodeState) error {
	traversalStatus := n.TraversalStatus
	if traversalStatus == "" {
		traversalStatus = n.Status
	}
	rowArgs := []driver.Value{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, n.Path, n.ParentPath,
		n.Type, n.Size, n.MTime, int32(n.Depth), traversalStatus, n.CopyStatus, n.Excluded, n.Errors,
	}
	switch table {
	case "DST":
		if aw.dstNodes != nil {
			return aw.dstNodes.AppendRow(rowArgs...)
		}
		return nil
	default:
		if aw.srcNodes != nil {
			return aw.srcNodes.AppendRow(rowArgs...)
		}
		return nil
	}
}

func (aw *queueAppenderWriter) Flush() error {
	for _, a := range []*duckdb.Appender{aw.srcStaging, aw.dstStaging, aw.srcNodes, aw.dstNodes} {
		if a != nil {
			if err := a.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (aw *queueAppenderWriter) Close() error {
	var err error
	for _, p := range []**duckdb.Appender{&aw.srcStaging, &aw.dstStaging, &aw.srcNodes, &aw.dstNodes} {
		if *p != nil {
			if e := (*p).Close(); e != nil {
				err = e
			}
			*p = nil
		}
	}
	return err
}
