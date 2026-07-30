// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import (
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// MapFSOperation maps Sylos-FS adapter operation strings to FSOperation.
func MapFSOperation(operation string) FSOperation {
	switch operation {
	case "ListChildren":
		return OpListChildren
	case "CreateFolder":
		return OpCreateFolder
	case "DeleteNode", "DeleteBatch", "DeleteFile", "DeleteFolder":
		return OpDelete
	case "OpenRead":
		return OpDownload
	case "CreateFileUpload", "UploadFile", "OpenWrite":
		return OpUpload
	default:
		return ""
	}
}

// ClassifyFSOperation returns download vs upload for copy-related operation names.
func ClassifyFSOperation(operation string) FSOperation {
	switch operation {
	case "OpenRead":
		return OpDownload
	case "CreateFileUpload", "UploadFile", "OpenWrite":
		return OpUpload
	case "CreateFolder":
		return OpCreateFolder
	case "ListChildren":
		return OpListChildren
	case "DeleteNode", "DeleteBatch", "DeleteFile", "DeleteFolder":
		return OpDelete
	default:
		return MapFSOperation(operation)
	}
}

// DegradationAppliesToOperation reports whether a degradation signal matches active ops.
func DegradationAppliesToOperation(signalOp string, active []FSOperation) bool {
	mapped := ClassifyFSOperation(signalOp)
	if mapped == "" {
		return true
	}
	for _, op := range active {
		if op == mapped {
			return true
		}
	}
	return false
}

// AdaptersForScaling holds fallback FS adapters when queue-local adapters are unset.
type AdaptersForScaling struct {
	Src fstypes.FSAdapter
	Dst fstypes.FSAdapter
}
