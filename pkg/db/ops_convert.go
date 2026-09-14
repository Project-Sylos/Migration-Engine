// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func sideFromQueue(table string) string {
	if table == "DST" {
		return opsdb.SideDST
	}
	return opsdb.SideSRC
}

func nodeToOps(n *NodeState) opsdb.NodeRecord {
	if n == nil {
		return opsdb.NodeRecord{}
	}
	return opsdb.NodeRecord{
		ID:              n.ID,
		ServiceID:       n.ServiceID,
		ParentID:        n.ParentID,
		ParentServiceID: n.ParentServiceID,
		Path:            n.Path,
		ParentPath:      n.ParentPath,
		Name:            n.Name,
		DisplayPath:     n.DisplayPath,
		Type:            n.Type,
		Size:            n.Size,
		MTime:           n.MTime,
		Depth:           n.Depth,
		IncludeOnly:     n.IncludeOnly,
		GPLState:        n.GPLState,
	}
}

func statusFromNode(n *NodeState) opsdb.StatusRecord {
	if n == nil {
		return opsdb.StatusRecord{}
	}
	trav := n.TraversalStatus
	if trav == "" {
		trav = n.Status
	}
	st := opsdb.StatusRecord{
		TraversalStatus: trav,
		CopyStatus:      n.CopyStatus,
		DeleteStatus:    n.DeleteStatus,
		GPLStatus:       n.GPLStatus,
		IncludeOnly:     n.IncludeOnly,
	}
	return st
}

func statusFromEvent(e StatusEvent) opsdb.StatusRecord {
	excl := e.ExclusionSource
	// Terminal rediscovery / copy-retry clears retry marks so a later pull does not
	// re-apply mark-scoped review deltas.
	if opsdb.IsDiscoveryRetryMark(excl) &&
		(e.TraversalStatus == StatusSuccessful || e.TraversalStatus == StatusFailed) {
		excl = ""
	}
	if opsdb.IsCopyRetryMark(excl) && e.CopyStatus != "" &&
		e.CopyStatus != CopyStatusPending {
		excl = ""
	}
	return opsdb.StatusRecord{
		TraversalStatus:   e.TraversalStatus,
		CopyStatus:        e.CopyStatus,
		DeleteStatus:      e.DeleteStatus,
		GPLStatus:         e.GPLStatus,
		ExclusionSource:   excl,
		DeterminingRuleID: e.DeterminingRuleID,
		ErrorLogID:        e.ErrorLogID,
	}
}

func applyStatusToNode(n *NodeState, st opsdb.StatusRecord) {
	if n == nil {
		return
	}
	if st.TraversalStatus != "" {
		n.TraversalStatus = st.TraversalStatus
		n.Status = st.TraversalStatus
	}
	if st.CopyStatus != "" {
		n.CopyStatus = st.CopyStatus
	}
	if st.DeleteStatus != "" {
		n.DeleteStatus = st.DeleteStatus
	}
	if st.IncludeOnly != "" {
		n.IncludeOnly = st.IncludeOnly
	}
	n.ExclusionSource = st.ExclusionSource
}

func idMapToOps(e IDMapEvent) opsdb.IDMapRecord {
	return opsdb.IDMapRecord{
		SrcID:  e.SrcInternalID,
		DstID:  e.DstInternalID,
		Source: e.Source,
		Status: e.Status,
	}
}
