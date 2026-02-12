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

func (aw *queueAppenderWriter) appendNode(table string, n *NodeState, rowArgs []driver.Value) error {
	traversalStatus := n.TraversalStatus
	if traversalStatus == "" {
		traversalStatus = n.Status
	}
	rowArgs[0] = n.ID
	rowArgs[1] = n.ServiceID
	rowArgs[2] = n.ParentID
	rowArgs[3] = n.ParentServiceID
	rowArgs[4] = n.Path
	rowArgs[5] = n.ParentPath
	rowArgs[6] = n.Type
	rowArgs[7] = n.Size
	rowArgs[8] = n.MTime
	rowArgs[9] = int32(n.Depth)
	rowArgs[10] = traversalStatus
	rowArgs[11] = n.CopyStatus
	rowArgs[12] = n.Excluded
	rowArgs[13] = n.Errors
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
